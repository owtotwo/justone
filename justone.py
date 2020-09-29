"""
Fast duplicate file finder.
Usage: duplicates.py <folder> [<folder>...]

Based on https://stackoverflow.com/a/36113168/300783
Modified for Python3 with some small code improvements.
"""
import hashlib
from io import BufferedReader
import os
import sys
import stat
from os import scandir, DirEntry, PathLike
from collections import defaultdict
from sys import path
from typing import DefaultDict, Iterable, Iterator, List, Sequence, Union, AnyStr
from pathlib import Path
from tqdm import tqdm

import knives


class JustOneError(Exception):
    """ Base Exception for this module. """


class FindDuplicatesError(JustOneError):
    """ Error when find_duplicates() running. """


def get_hash(fp: Path, first_chunk_only: bool = False, hash_algo=hashlib.sha1) -> bytes:
    def chunk_reader(freader: BufferedReader, chunk_size: int = 1024) -> Iterator[bytes]:
        """ Generator that reads a file in chunks of bytes """
        while True:
            chunk = freader.read(chunk_size)
            if not chunk:
                return
            yield chunk

    hashobj = hash_algo()
    with fp.open(mode='rb') as f:
        if first_chunk_only:
            hashobj.update(f.read(1024))
        else:
            for chunk in chunk_reader(f):
                hashobj.update(chunk)
    return hashobj.digest()


def find_duplicates(fps_or_dp: Union[Iterable[Path], Path, str]) -> Iterable[Sequence[Path]]:
    """ 查重文件

    输入文件可迭代对象如 fps = (fp for fp in Path.cwd().iterdir() if fp.is_file())
    或文件夹对象 dp = Path(r'D:\folder')
    或文件夹路径 dp = r'D:\folder'
    输出重复文件列表 [[a_1, a_2], [b_1, b_2, b_3]]
    """
    def scan_dir(dp: Union[AnyStr, PathLike]) -> Iterator[DirEntry]:
        with scandir(dp) as it:
            for entry in it:
                if entry.is_dir():
                    for e in scan_dir(entry.path):
                        yield e
                else:
                    yield entry

    files_by_size: DefaultDict[int, List[Path]] = defaultdict(list)
    files_by_small_hash: DefaultDict[bytes, List[Path]] = defaultdict(list)
    files_by_full_hash: DefaultDict[bytes, List[Path]] = defaultdict(list)
    is_argument_iterable: bool = False
    if isinstance(fps_or_dp, str):
        dp = Path(fps_or_dp)
        if not dp.is_dir():
            raise FindDuplicatesError(f'{fps_or_dp}不是合法的文件夹路径')
    elif isinstance(fps_or_dp, Path):
        dp = fps_or_dp
        if not dp.is_dir():
            raise FindDuplicatesError(f'{fps_or_dp}不是合法的文件夹路径')
    elif isinstance(fps_or_dp, Iterable):
        is_argument_iterable = True
        dp = None
    else:
        raise FindDuplicatesError(f'第一个参数类型错误')
    if is_argument_iterable:
        fps = fps_or_dp
        for fp in tqdm(tuple(fps)):
            try:
                # if the target is a symlink (soft one), do not dereference it.
                fp = fp.absolute()
                fp_stat = fp.stat()
                is_file = stat.S_ISREG(fp_stat.st_mode)
                if not is_file:
                    continue
                file_size = fp_stat.st_size
            except OSError as e:
                # not accessible (permissions, etc) - pass on
                raise FindDuplicatesError from e
                continue
            files_by_size[file_size].append(fp)
    else:
        try:
            for entry in scan_dir(dp):
                file_size = entry.stat().st_size
                files_by_size[file_size].append(Path(entry.path))
        except OSError as e:
            # not accessible (permissions, etc)
            raise FindDuplicatesError from e
    # For all files with the same file size, get their hash on the first 1024 bytes
    for file_size, fps in tqdm(tuple(files_by_size.items())):
        if len(fps) < 2:
            continue # this file size is unique, no need to spend cpu cycles on it
        for fp in fps:
            try:
                small_hash = get_hash(fp, first_chunk_only=True)
            except OSError as e:
                # the file access might've changed till the exec point got here
                raise FindDuplicatesError from e
                continue
            files_by_small_hash[(file_size, small_hash)].append(fp)
    # For all files with the hash on the first 1024 bytes, get their hash on the full
    # file - collisions will be duplicates
    for fps in tqdm(tuple(files_by_small_hash.values())):
        if len(fps) < 2:
            # the hash of the first 1k bytes is unique -> skip this file
            continue
        for fp in fps:
            try:
                full_hash = get_hash(fp, first_chunk_only=False)
            except OSError as e:
                # the file access might've changed till the exec point got here
                raise FindDuplicatesError from e
                continue
            files_by_full_hash[full_hash].append(fp)
    return filter(lambda x: len(x) > 1, files_by_full_hash.values())


@knives.measure_time
def print_duplicates(dp: Path):
    try:
        duplicates_list = find_duplicates(dp)
    except FindDuplicatesError as e:
        print(f'Error: {knives.format_exception_chain(e)}')
        return 1
    for duplicates in duplicates_list:
        print(f'Duplicate found:')
        for fp in duplicates:
            print(f' - {fp}')
        print(f'')
    return 0


@knives.measure_profile
def main():
    def print_usage():
        print(f'Usage: justone <folder_path>')

    if len(sys.argv) == 1:
        print_usage()
        return 0
    elif len(sys.argv) > 2:
        print_usage()
        return 1
    path_str = sys.argv[1]
    if path_str in ('-h', '--help'):
        print_usage()
        return 0
    dp = Path(path_str)
    if not dp.is_dir():
        print(f'Not an existed folder path.')
        print_usage()
        return
    return print_duplicates(dp)


@knives.measure_profile
def test():
    dp = Path(r'X:\music(deprecated)')
    print_duplicates(dp)


def test_dig_file_methods():
    @knives.measure_time
    def test_walk(dp):
        def walk_dir(dp):
            for dirpath, _, filenames in os.walk(dp):
                dirpath = Path(dirpath)
                for filename in filenames:
                    yield dirpath / filename

        print(len(list(walk_dir(dp))))

    @knives.measure_time
    def test_rglob(dp):
        print(len(list(dp.rglob('*'))))

    @knives.measure_time
    def test_iterdir(dp):
        def iter_dir(dp):
            for p in dp.iterdir():
                if p.is_dir():
                    for i in iter_dir(p):
                        yield i
                else:
                    yield p

        print(len(list(iter_dir(dp))))

    @knives.measure_time
    def test_scandir(dp):
        def scan_dir_raw(dp: Union[AnyStr, PathLike]) -> Iterator[DirEntry]:
            with os.scandir(dp) as it:
                for entry in it:
                    if entry.is_dir():
                        for e in scan_dir_raw(entry.path):
                            yield e
                    else:
                        yield entry

        def scan_dir(dp):
            for entry in scan_dir_raw(dp):
                yield Path(entry.path)

        print(len(list(scan_dir(dp))))

    dp = Path(r'X:\music(deprecated)')
    test_walk(dp)
    test_rglob(dp)
    test_iterdir(dp)
    test_scandir(dp)


if __name__ == '__main__':
    # sys.exit(main())
    test()
