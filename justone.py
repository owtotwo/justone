"""
Duplicate files finder.
Usage: duplicates.py <folder> [<folder>...]

Inspired by https://stackoverflow.com/a/36113168/300783
"""
import hashlib
import stat
import sys
from collections import defaultdict
from io import BufferedReader
from os import DirEntry, PathLike, scandir
from pathlib import Path
from typing import AnyStr, DefaultDict, Final, Iterable, Iterator, List, Sequence, Union

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = lambda x: x

__author__: Final[str] = 'owtotwo'
__copyright__: Final[str] = 'Copyright 2020 owtotwo'
__credits__: Final[Sequence[str]] = ['owtotwo']
__license__: Final[str] = 'LGPLv3'
__version__: Final[str] = '0.0.1'
__maintainer__: Final[str] = 'owtotwo'
__email__: Final[str] = 'owtotwo@163.com'
__status__: Final[str] = 'Experimental'


class JustOneError(Exception):
    """ Base Exception for this module. """


class FindDuplicatesError(JustOneError):
    """ Error when find_duplicates() running. """


# return string like '[aaa] -> [bbb] -> [ccc]'
def format_exception_chain(e: BaseException):
    # recursive function, get exception chain from __cause__
    def get_exception_chain(e: BaseException) -> List[BaseException]:
        return [e] if e.__cause__ is None else [e] + get_exception_chain(e.__cause__)

    return ''.join(f'[{exc}]' if i == 0 else f' -> [{exc}]' for i, exc in enumerate(reversed(get_exception_chain(e))))


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


if __name__ == '__main__':
    sys.exit(main())
