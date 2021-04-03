from __future__ import print_function
import os
import hashlib
import sqlite3
import logging
from datetime import datetime, timedelta
import gevent
import gevent.queue
from gevent import monkey

monkey.patch_all(thread=False, select=False)
# ssl-related modules have to be imported after monkey-patching to get rid of the error:
# MonkeyPatchWarning: Monkey-patching ssl after ssl has already been imported may lead to errors
# PEP 8: E402 module level import not at top of file
import requests # noqa # To get rid of PEP 8: E402 module level import not at top of file


class BatchFileDownloadCache(object):
    CACHE_VALID_DAYS = 3
    sql_tpl_create_table = 'CREATE TABLE IF NOT EXISTS %s (' \
                           'ID integer NOT NULL primary key autoincrement, gmt_created text NOT NULL,' \
                           'url nvarchar(256) NOT NULL, file_path_relative nvarchar(256) NOT NULL,' \
                           'file_length integer NOT NULL' \
                           ')'
    sql_time_format = "%Y-%m-%dT%H:%M:%SZ"
    default_sqlite3_file_name = 'cache.sqlite3'
    default_sqlite3_table_name = 'cache'

    def __init__(self, prefix, table_name=None, sqlite3_file_name=None):
        assert (os.path.isdir(prefix)), "Cache prefix folder shall exist: %s" % prefix
        self.prefix = prefix
        self.table_name = table_name if table_name else self.default_sqlite3_table_name
        self.sqlite3_file_name = sqlite3_file_name if sqlite3_file_name else self.default_sqlite3_file_name
        self.db = os.path.join(prefix, self.sqlite3_file_name)
        self.db_conn = sqlite3.connect(self.db)
        logging.info("will init a cache with prefix=%s, sqlite3 db=%s, table=%s" % (
            prefix, sqlite3_file_name, table_name))
        self.__db_create_table_if_not_exists__()

    def get_table_name(self):
        return self.table_name

    def __db_create_table_if_not_exists__(self):
        cu = self.db_conn.cursor()
        cu.execute(self.sql_tpl_create_table % self.get_table_name())

    @staticmethod
    def is_url_valid_and_safe(url):
        if not (url.startswith('http://') or url.startswith('https://')):
            return False
        p = url.index('://') + 3
        rest = url[p:]
        for c in rest:
            if 'a' <= c <= 'z' or 'A' <= c <= 'Z' or '0' <= c <= '9' or c in '/=-_:,.':
                pass
            else:
                return False
        return True

    @staticmethod
    def cache_file_data_checksum(data):
        return hashlib.md5(data).hexdigest()

    def cache_drop(self, url):
        self.cache_get(url, drop_it=True)

    def cache_get(self, url, drop_it=False, verify_checksum=True):
        cu = self.db_conn.cursor()
        cu.execute('SELECT * FROM %s WHERE url = "%s"' % (self.get_table_name(), url))
        r = cu.fetchone()
        if not r:
            return None
        r = list(r)
        _, gmt_created, _, file_path_relative, file_length = r
        if datetime.strptime(gmt_created, self.sql_time_format) + timedelta(
                days=self.CACHE_VALID_DAYS) < datetime.utcnow():
            drop_it = True
        file_path = self.prefix + '/' + file_path_relative
        if not os.path.isfile(file_path):  # Cache marked externally as invalidated by simply removing it
            logging.info('Cache for URL "%s" points to an non-existing file "%s"; will evict it' % (url, file_path))
            drop_it = True
        if not drop_it:
            with open(file_path, 'rb') as fh:
                data = fh.read()
                broken = None
                if len(data) != file_length:
                    broken = "file length"
                elif verify_checksum:
                    checksum = self.cache_file_data_checksum(data)
                    file_name = file_path.split('/')[-1]
                    if checksum != file_name:
                        broken = "file checksum"
                if broken:
                    logging.warning('%s: got a %s inconsistency at url "%s" file "%s"; will drop it' % (
                        self.__class__.__name__, broken, url, file_path))
                    drop_it = True
                else:
                    logging.debug('Cache hit for URL: %s; file_path=%s, size=%d' % (url, file_path, file_length))
                    return data
        if drop_it:
            cu.execute('DELETE FROM %s WHERE url = "%s"' % (self.get_table_name(), url))
            self.db_conn.commit()
            if os.path.isfile(file_path):
                os.remove(file_path)
        return None

    def cache_put(self, url, data):
        if len(data) == 0:
            logging.error("refuses to cache empty data for URL %s" % url)
            return
        if not self.is_url_valid_and_safe(url):  # to ensure uniqueness
            logging.warning('refuses to cache data for an invalid URL: ' + url)
            return
        t = datetime.utcnow().strftime(self.sql_time_format)
        cu = self.db_conn.cursor()
        file_folder_relative = url[url.index('://')+3:]  # the part of URL starting from hostname
        file_folder = self.prefix + '/' + file_folder_relative
        if os.path.exists(file_folder):
            assert os.path.isdir(file_folder)
        else:
            os.makedirs(file_folder)
        file_name = self.cache_file_data_checksum(data)
        # Don't use os.path.sep here to help separate and extract file_name later.
        # Both separators are tolerated any way.
        file_path_relative = file_folder_relative + '/' + file_name
        file_path = file_folder + '/' + file_name
        with open(file_path, 'wb') as fh:
            fh.write(data)
        cu.execute('INSERT INTO %s VALUES (?, ?, ?, ?, ?)' % self.get_table_name(),
                   (None, t, url, file_path_relative, len(data)))
        self.db_conn.commit()
        logging.info('has cached data for URL: ' + url)


def http_headers_set_default(headers, k, v):
    if k not in headers:
        headers[k] = v


def http_headers_parse_and_amend(header_list):
    headers = {}
    for s in header_list:
        d = s.split(':', 1)
        k = d[0].strip()
        v = d[1].strip()
        if k in headers:
            logging.warning('Duplicated http-header: key=%s, values are "%s" and "%s"; behavior undefined' % (
                k, headers[k], v))
        headers[k] = v
    http_headers_set_default(headers, 'User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                                    '(KHTML, like Gecko) Chrome/74.0.3729.11 Safari/537.36')
    return headers


class DownloadWorker(object):
    PROXY_DIRECT = 'direct'
    # See init() for details
    queue_cache_mgr = None
    queue_jobs_todo = None

    @classmethod
    def commit_job(cls, job, data):
        url = job[0]
        while True:
            try:  # try to get it; on entering critical section
                cache_manager = cls.queue_cache_mgr.get(block=True, timeout=1)
            except gevent.queue.Empty:
                continue
            try:
                cache_manager.cache_put(url, data)
            except Exception as e:
                logging.error('cache_manager.cache_put() failed for URL "%s": %s' % (url, str(e)))
                return
            cls.queue_cache_mgr.put(cache_manager)  # put it back; out of critical section
            break

    def __init__(self, name, proxy, *args, **kwargs):
        self.name = name
        self.proxy = proxy
        super(DownloadWorker, self).__init__(*args, **kwargs)

    def __str__(self):
        return '%s "%s" using proxy %s' % (self.__class__.__name__, self.name, self.proxy)

    def download(self):
        proxies = None if self.proxy == 'direct' or self.proxy is None else {
            'http': self.proxy,
            'https': self.proxy,
        }
        session = requests.Session()
        while len(self.queue_jobs_todo) > 0:
            try:
                job = self.queue_jobs_todo.get(block=True, timeout=1)
            except gevent.queue.Empty:
                continue
            url = job[0]
            headers = job[1]
            tries = 5
            while True:
                try:
                    # TCP session re-use borrowed from https://stackoverflow.com/a/34491383 . With that we don't really
                    # need to implement the logic to re-use TLS sessions.
                    res = session.get(url, headers=headers, proxies=proxies)
                    code = res.status_code
                    if code == 200:
                        logging.info("%s: request for URL %s is successful; will commit it" % (self, url))
                        self.commit_job(job, res.content)
                    else:
                        logging.info("%s: request for URL %s is unsuccessful; result=%s" % (self, url, res))
                    break  # The request itself is successful
                except Exception as e:
                    logging.info("%s: request for URL %s failed with exception %s" % (self, url, str(e)))
                    tries -= 1
                    if tries == 0:
                        # Network is down, etc.
                        raise RuntimeError('will stop working after two many failure for URL "%s"' % url)

    @classmethod
    def init(cls, cache_manager: BatchFileDownloadCache, jobs):
        """
        To initialize shared data for the DownloadWorker class. It is designed that the class may be readily
        re-initialized to work upon another job set by simply calling init() again.
        :param cache_manager: instance of cache.FileDownloadCacheManager
        :param jobs: list of download jobs to perform; list or tuple of (url, headers)
        :return: None
        """
        cls.queue_jobs_todo = gevent.queue.Queue()
        for j in jobs:
            cls.queue_jobs_todo.put(j)
        # We use a gevent queue to hold cache_manager and to implement critical section.
        cls.queue_cache_mgr = gevent.queue.Queue()
        cls.queue_cache_mgr.put(cache_manager)

    @classmethod
    def init_and_submit_direct_jobs(cls, prefix, jobs, threads=6, sqlite3_table_name=None, sqlite3_db_name=None):

        """
        The all-in-one entry point to create a number of worker units and submit the jobs that makes of direct
        remote connections (without proxies).
        @param prefix: the output prefix
        @param jobs: iterable of jobs; each job is tuple of (url, http-headers).
        @param threads: number of threads
        @param sqlite3_table_name
        @param sqlite3_db_name
        @return None
        """
        cache_mgr = BatchFileDownloadCache(prefix, sqlite3_table_name, sqlite3_db_name)
        jobs_filtered = []
        # About the per-job http-headers. Each of the requested_formats got its own http-headers,
        # whereas each represents a track and hence a large number of URLs.
        # By defining http headers as job wise, we allow each job to have a special one, which may become useful later.
        # To allow maximum performance, we don't try to make a copy and modify. Consider that our caller may have only
        # one copy of http-headers dict with the necessary user-agent keys then pass it around,
        # our approach is just fine.
        for job in jobs:
            url, headers = job
            url = url.strip()
            http_headers_set_default(headers, 'User-Agent',
                                     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                                     ' (KHTML, like Gecko) Chrome/74.0.3729.11 Safari/537.36')
            if cache_mgr.cache_get(url):
                pass
            elif cache_mgr.is_url_valid_and_safe(url):
                jobs_filtered.append([url, headers])
            else:
                logging.warning('URL %s cannot be cached reliably; skipped' % url)

        cls.init(cache_mgr, jobs_filtered)
        gevent_jobs = []
        for i in range(0, 1):  # direct-only
            for j in range(0, threads):
                w = cls('%d-%d' % (i, j), None)  #
                gevent_jobs.append(gevent.spawn(w.download))
        gevent.wait(gevent_jobs)
