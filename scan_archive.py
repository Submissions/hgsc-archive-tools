#!/usr/bin/env python3

"""Scan either the manual or automatic archive. In the current working
directory create separate TSV files for each month of archive data."""

from argparse import ArgumentParser
import csv
import logging
from operator import attrgetter
from os import fsdecode, path, readlink, scandir
import re

from archive_patterns import ArchivePattern
from wh_lib import TsvDialect


logger = logging.getLogger(__name__)

MANUAL_ARCHIVE_ROOT = '/stornext/snfsa/archive'
AUTO_ARCHIVE_ROOT = '/stornext/snfsa/archive/archive_jobs'
MANUAL_DIR_NAME_PAT = re.compile(r'\d{12}')
STAT_FIELDS = '''
st_mode
st_ino
st_dev
st_nlink
st_uid
st_gid
st_size
st_atime
st_mtime
st_ctime
'''.split()


def main():
    args = parse_args()
    config_logging(args)
    run(args.archive)


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('archive',
                        choices=['auto', 'manual'],
                        help='which archive')
    args = parser.parse_args()
    return args


def config_logging(args):
    global logger
    format = '%(levelname)-7s %(asctime)-15s %(message)s'
    logging.basicConfig(level=logging.INFO,
                        format=format,
                        filename=args.archive+'.log')
    logger = logging.getLogger('scan_archive')


def run(archive):
    logger.info('scanning: %s', archive)
    assert archive == 'manual'
    scan_manual_archive()


def scan_manual_archive():
    """Scan the manual archive and dump monthly manifest file to the current
    working directory."""
    with ManifestWriter('manual') as writer:
        for entry in iterate_manual_archive():
            writer.write_record(entry)


class ManifestWriter:
    """Context mananager that writes manifest records to multiple TSV files
    in the current working directory based on the month that the file was
    archived. Insures that manifest files are closed promptly. Once
    constructed, everything flows through the `write_record` method."""
    def __init__(self, archive):
        assert archive in ('auto', 'manual')
        self.archive = archive
        if self.archive == 'auto':
            self.pattern = ArchivePattern.AUTO_PATTERN
        else:
            self.pattern = ArchivePattern.MANUAL_PATTERN
        self.current_file = None
        # Clear current_month, current_file_name, current_writer:
        self._close_month()
        with open(archive + '-0000-00.tsv', 'w') as header_file:
            header = [f[3:] for f in STAT_FIELDS] + ['path']
            print(*header, sep='\t', end='\n', file=header_file)
        self.links_file = open(archive + '-symlinks.tsv', 'w')

    def __enter__(self):
        """Context Manager support, returns `self`."""
        return self

    def __exit__(self, *exc):
        """Context Manager support, calls `close()`"""
        self.close()
        return False

    def close(self):
        self._close_month()
        self.links_file.close()
        self.links_file = None

    def write_record(self, entry):
        """entry is `os.DirEntry` describing an archived file. Write to a
        file corresponding to the month of the archive job as encoded in
        `entry.path`."""
        if entry.is_symlink():
            self._write_symlink_record(entry)
        else:
            self._write_file_record(entry)

    def _write_symlink_record(self, entry):
        path = entry.path
        try:
            target = readlink(path)
        except Exception as e:
            logger.exception('exception during readlink')
        else:
            print(path, target, sep='\t', end='\n', file=self.links_file)

    def _write_file_record(self, entry):
        assert entry.is_file(follow_symlinks=False)
        path = entry.path
        archive_time_str, archive, original_path = self.pattern.match(path)
        if not (archive_time_str and archive and original_path):
            logger.error('failed parse: %s', path)
            return
        assert archive == self.archive
        month = archive_time_str[:7]
        if month != self.current_month:
            self._open_new_month(month)
        try:
            s = entry.stat()
        except Exception as e:
            logger.exception('exception during stat')
        else:
            row = [round(getattr(s, f)) for f in STAT_FIELDS] + [entry.path]
            self.current_writer.writerow(row)

    def _open_new_month(self, month):
        logger.info('new_month %s', month)
        self._close_month()
        self.current_month = month
        self.current_file_name = '{}-{}.tsv'.format(self.archive,
                                                    self.current_month)
        self.current_file = open(self.current_file_name, 'w',
                                 newline='', encoding='utf-8')
        self.current_writer = csv.writer(self.current_file, dialect=TsvDialect)

    def _close_month(self):
        if self.current_file:
            self.current_file.close()
        self.current_month = None
        self.current_file_name = None
        self.current_file = None
        self.current_writer = None


def iterate_manual_archive():
    """Iterates contents of the manual archive in order of the archive
    job date."""
    with scandir(MANUAL_ARCHIVE_ROOT) as top_contents:
        for entry in sorted(top_contents, key=attrgetter('name')):
            if entry.is_dir(follow_symlinks=False):
                if MANUAL_DIR_NAME_PAT.match(entry.name):
                    yield from iterate_files(entry)


def iterate_files(directory_pathlike):
    """Generator that recursively iterates all the files and symlinks. Does not
    follow symlinks. Logs anything that is not a symlink, file or directory
    as a WARNING. Yields `os.DirEntry` objects which are `os.PathLike`."""
    try:
        with scandir(directory_pathlike) as iter_contents:
            for entry in iter_contents:
                if entry.is_dir(follow_symlinks=False):
                    yield from iterate_files(entry)
                elif entry.is_file(follow_symlinks=False):
                    yield entry
                elif entry.is_symlink():
                    yield entry
                else:
                    logger.warning('unexpected object type: %r', fsdecode(entry))
    except Exception as e:
        logger.exception('exception during scandir')


if __name__ == '__main__':
    main()
