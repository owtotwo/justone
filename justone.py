"""
Fast duplicate files finder.

Usage: justone.py [-h] [-s] [-v] FOLDER [FOLDER ...]

Inspired by https://stackoverflow.com/a/36113168/300783
"""
import itertools
import platform
import stat
import sys
import argparse
from collections import defaultdict
from io import BufferedReader
from os import DirEntry, PathLike, scandir
from pathlib import Path
from typing import AnyStr, Callable, DefaultDict, Dict, Final, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union

try:
    import xxhash

    # # This can return an incorrect result if 32bit Python is running on a 64bit operating system.
    # is_64bits = sys.maxsize > 2**32
    if platform.machine() in {'x86_64', 'AMD64'}:
        _hash_func_default: Callable = xxhash.xxh3_64
    else:
        _hash_func_default: Callable = xxhash.xxh32
except ModuleNotFoundError:
    import hashlib
    _hash_func_default: Callable = hashlib.sha1

try:
    from tqdm import tqdm
    tqdm2: Callable = lambda x: tqdm(tuple(x), ascii=False)
except ModuleNotFoundError:
    tqdm2: Callable = lambda x: x

__author__: Final[str] = 'owtotwo'
__copyright__: Final[str] = 'Copyright 2020 owtotwo'
__credits__: Final[Sequence[str]] = ['owtotwo']
__license__: Final[str] = 'LGPLv3'
__version__: Final[str] = '0.1.2'
__maintainer__: Final[str] = 'owtotwo'
__email__: Final[str] = 'owtotwo@163.com'
__status__: Final[str] = 'Experimental'

HASH_FUNCTION_DEFAULT: Final[Callable] = _hash_func_default
SMALL_HASH_CHUNK_SIZE_DEFAULT: Final[int] = 1024


# return string like '[aaa] -> [bbb] -> [ccc]'
def format_exception_chain(e: BaseException) -> str:
    # recursive function, get exception chain from __cause__
    def get_exception_chain(e: BaseException) -> List[BaseException]:
        return [e] if e.__cause__ is None else [e] + get_exception_chain(e.__cause__)

    # use Exception class name as string if no message in Exception object
    e2s = lambda e: str(e) or e.__class__.__name__
    return ''.join(f'[{e2s(exc)}]' if i == 0 else f' -> [{e2s(exc)}]' for i, exc in enumerate(reversed(get_exception_chain(e))))


# TODO: add type hints for Type Alias
FileIndex = int # the index of file_info
FileSize = int # the number of bytes
HashValue = bytes # the return type of hash function
SinglePath = Union[str, Path] # same as open(SinglePath)
IterablePaths = Iterable[SinglePath]


class UnreachableError(RuntimeError):
    """ like unreachable in rust, which means the code will not reach expectedly. """


class JustOneError(Exception):
    """ Base Exception for this module. """


class GetFileInfoError(JustOneError):
    """ Exception for JustOne._get_file_info """


class UpdateFileInfoError(JustOneError):
    """ Exception for JustOne._update_file_info """


class UpdateError(JustOneError):
    """ Exception for JustOne._update_* """


class GetSmallHashError(JustOneError):
    """ Exception for JustOne._get_small_hash """


class GetFullHashError(JustOneError):
    """ Exception for JustOne._get_full_hash """


class JustOne:
    def __init__(self, hash_func: Callable = HASH_FUNCTION_DEFAULT) -> None:
        """
        file_info: [
          <Index, Path-Object, File-Size, Small-Hash, Full-Hash>
          [0, Path('D:/abc/efg.txt'), 16801, '900150983cd24fb0', 'd6963f7d28e17f72'],
          [1, Path(...), 14323, ..., ...],
          [2, ...],
          ...
        ]
        file_index: {
            <Path-Object: Index>
            Path('D:/abc/efg.txt'): 0,
            Path(...): 1,
            ...
        }
        """
        self.hash_func: Callable = hash_func
        self.file_info: List[Tuple[FileIndex, Path, FileSize, Optional[HashValue], Optional[HashValue]]] = []
        self.file_index: Dict[Path, FileIndex] = {}
        self.size_dict: DefaultDict[FileSize, Set[FileIndex]] = defaultdict(set)
        self.small_hash_dict: DefaultDict[Tuple[FileSize, HashValue], Set[FileIndex]] = defaultdict(set)
        self.full_hash_dict: DefaultDict[HashValue, Set[FileIndex]] = defaultdict(set)

    @staticmethod
    def _scan_dir(dp: Union[AnyStr, PathLike]) -> Iterator[DirEntry]:
        with scandir(dp) as it:
            for entry in it:
                if entry.is_dir():
                    for e in JustOne._scan_dir(entry.path):
                        yield e
                else:
                    yield entry

    @staticmethod
    def _get_hash(fp: Path,
                  first_chunk_only: bool = False,
                  first_chunk_size: int = SMALL_HASH_CHUNK_SIZE_DEFAULT,
                  hash_func: Callable = HASH_FUNCTION_DEFAULT) -> HashValue:
        """
        Calculate hash for file or just for first chunk of file(the fisrt 1024bytes).
        """
        def chunk_reader(freader: BufferedReader, chunk_size: int = 1024) -> Iterator[bytes]:
            """ Generator that reads a file in chunks of bytes """
            while True:
                chunk = freader.read(chunk_size)
                if not chunk:
                    return
                yield chunk

        hash_obj = hash_func()
        with fp.open(mode='rb') as f:
            if first_chunk_only:
                hash_obj.update(f.read(first_chunk_size))
            else:
                for chunk in chunk_reader(f, chunk_size=1024):
                    hash_obj.update(chunk)
        return hash_obj.digest()

    def _get_file_info(self, index: FileIndex) -> Tuple[Path, FileSize, Optional[HashValue], Optional[HashValue]]:
        """
        Get file info from self.file_info .
        """
        try:
            _, file, file_size, small_hash, full_hash = self.file_info[index]
        except IndexError as e:
            raise GetFileInfoError from e
        return file, file_size, small_hash, full_hash

    def _add_file_info(self,
                       file: Path,
                       file_size: Optional[FileSize] = None,
                       small_hash: Optional[HashValue] = None,
                       full_hash: Optional[HashValue] = None) -> FileIndex:
        """
        Add file info to self.file_info .
        If file is existed, do nothing.
        """
        index = self.file_index.get(file, None)
        if index is None:
            file_size = file.stat().st_size if file_size is None else file_size
            index = len(self.file_info)
            self.file_index[file] = index
            self.file_info.append((index, file, file_size, small_hash, full_hash))
        return index

    def _update_file_info(self,
                          index: FileIndex,
                          file_size: Optional[FileSize] = None,
                          small_hash: Optional[HashValue] = None,
                          full_hash: Optional[HashValue] = None) -> FileIndex:
        """
        Update file info to self.file_info, only support file-size, small-hash and full-hash.
        """
        try:
            index, file, file_size_old, small_hash_old, full_hash_old = self.file_info[index]
            file_size = file_size_old if file_size is None else file_size
            small_hash = small_hash or small_hash_old
            full_hash = full_hash or full_hash_old
            self.file_info[index] = (index, file, file_size, small_hash, full_hash)
        except IndexError as e:
            raise UpdateFileInfoError from e
        return index

    def _get_small_hash(self, index: FileIndex) -> HashValue:
        """
        If small hash is existed, use it. Otherwise, calculate the small hash, update it and return.
        """
        try:
            index, file, file_size, small_hash, full_hash = self.file_info[index]
        except IndexError as e:
            raise GetSmallHashError from e
        if small_hash is None:
            small_hash = self._get_hash(file, first_chunk_only=True, hash_func=self.hash_func)
            self.file_info[index] = (index, file, file_size, small_hash, full_hash)
        return small_hash

    def _get_full_hash(self, index: FileIndex) -> HashValue:
        """
        If full hash is existed, use it. Otherwise, calculate the full hash, update it and return.
        """
        try:
            index, file, file_size, small_hash, full_hash = self.file_info[index]
        except IndexError as e:
            raise GetFullHashError from e
        if full_hash is None:
            full_hash = self._get_hash(file, first_chunk_only=False, hash_func=self.hash_func)
            self.file_info[index] = (index, file, file_size, small_hash, full_hash)
        return full_hash

    def _merge_size_dict(self, size_dict_temp: Dict[FileSize, Set[FileIndex]]) -> Iterator[Tuple[FileSize, FileIndex]]:
        """
        Merge the new size-dict to self.size_dict .
        Return the file(with file-size) whose duplicates are existed.
        """
        for k, v in size_dict_temp.items():
            index_set = self.size_dict[k]
            index_set |= v
            if len(index_set) > 1:
                for file in v:
                    yield k, file

    def _merge_small_hash_dict(self, small_hash_dict_temp: Dict[Tuple[FileSize, HashValue], Set[FileIndex]]) -> Iterator[FileIndex]:
        """
        Merge the new small-hash-dict to self.small_hash_dict .
        Return the file whose duplicates are existed.
        """
        for k, v in small_hash_dict_temp.items():
            index_set = self.small_hash_dict[k]
            index_set |= v
            if len(index_set) > 1:
                for index in v:
                    yield index

    def _merge_full_hash_dict(self, full_hash_dict_temp: DefaultDict[HashValue, Set[FileIndex]]) -> Iterator[FileIndex]:
        """
        Merge the new full-hash-dict to self.full_hash_dict .
        Return the file whose duplicates are existed.
        """
        for k, v in full_hash_dict_temp.items():
            index_set = self.full_hash_dict[k]
            index_set |= v
            if len(index_set) > 1:
                for index in v:
                    yield index

    def _update_multiple_files_with_size(self, files_with_size: Iterable[Tuple[Path, FileSize]]) -> Set[FileIndex]:
        """
        Core function for update new files to JustOne object.
        """
        size_dict_temp: DefaultDict[FileSize, Set[FileIndex]] = defaultdict(set)
        small_hash_dict_temp: DefaultDict[Tuple[FileSize, HashValue], Set[FileIndex]] = defaultdict(set)
        full_hash_dict_temp: DefaultDict[HashValue, Set[FileIndex]] = defaultdict(set)
        duplicate_files_index: Set[FileIndex] = set()
        for file, file_size in tqdm2(files_with_size):
            file_index = self._add_file_info(file, file_size=file_size)
            size_dict_temp[file_size].add(file_index)
        for file_size, file_index in tqdm2(self._merge_size_dict(size_dict_temp)):
            try:
                small_hash = self._get_small_hash(file_index)
            except OSError as e: # TODO: replace with more specific Exceptions
                # the file access might've changed till the exec point got here
                raise UpdateError from e
            small_hash_dict_temp[(file_size, small_hash)].add(file_index)
        # For all files with the hash on the first 1024 bytes, get their hash on the full
        # file - collisions will be duplicates
        for file_index in tqdm2(self._merge_small_hash_dict(small_hash_dict_temp)):
            try:
                full_hash = self._get_full_hash(file_index)
            except OSError as e: # TODO: replace with more specific Exceptions
                # the file access might've changed till the exec point got here
                raise UpdateError from e
            full_hash_dict_temp[full_hash].add(file_index)
        for file_index in tqdm2(self._merge_full_hash_dict(full_hash_dict_temp)):
            duplicate_files_index.add(file_index)
        return duplicate_files_index

    def _update_multiple_files(self, files: IterablePaths) -> Set[FileIndex]:
        """
        Update multiple regular files to JustOne object.
        """
        files_with_size: List[Tuple[Path, FileSize]] = []
        for file in tqdm2(files):
            file = Path(file)
            file_stat = file.stat()
            is_reg = stat.S_ISREG(file_stat.st_mode) # TODO: is symlink ...
            if not is_reg:
                raise UpdateError(f'Not a Regular File: {file}')
            file_size = file_stat.st_size
            files_with_size.append((file, file_size))
        return self._update_multiple_files_with_size(files_with_size)

    def _update_single_directory(self, single_dir: SinglePath) -> Set[FileIndex]:
        """
        Update one directory(all inner files recursively) to JustOne object.
        """
        try:
            files_with_size = ((Path(entry.path), entry.stat().st_size) for entry in tqdm2(JustOne._scan_dir(single_dir)))
        except NotADirectoryError as e:
            # From JustOne._scan_dir
            raise UpdateError from e
        except OSError as e: # TODO: replace with more specific Exceptions
            # not accessible (permissions, etc)
            raise UpdateError from e
        return self._update_multiple_files_with_size(files_with_size)

    def _update_multiple_directories(self, dirs: IterablePaths) -> Set[FileIndex]:
        """
        Update multiple directories(all inner files recursively) to JustOne object.
        """
        duplicate_files: Set[FileIndex] = set()
        for d in dirs:
            duplicate_files |= self._update_single_directory(d)
        return duplicate_files

    # No use for the time being.
    def _update_single_file(self, single_file: SinglePath) -> Set[FileIndex]:
        """
        Update one file to JustOne object.
        """
        return self._update_multiple_files((single_file, ))

    def update(self, arg: Union[SinglePath, IterablePaths], *args: Union[SinglePath, IterablePaths]) -> 'JustOne':
        """
        The main api for JustOne object to update files.

        Could be chain calling like `JustOne(hashlib.sha1).update('D:\\data').update(Path('C:\\Wegame')).duplicates()`.
        Return files whose duplicates are existed.
        e.g.:
          1. update(file_1, file_2, file_3)    [Not Recommended]: file stat() calling which is too slow
          2. update(iterable_files)            [Not Recommended]: ditto
            - update([file_1, file_2])
            - update((f for f in Path.cwd().glob('*') if f.is_file()))  # Note: This is a slow method because of f.is_file()
          3. update(dir_1, dir_2, dir_3)       [Recommended]
          4. update(iterable_dirs)             [Recommended]
            - update([dir_1, dir_2])
            - update((f for f in Path.cwd().iterdir() if f.is_dir()))  # Note: This is a slow method because of f.is_dir()
        
        * Not support mix file and directory as arguments, such as update(file_1, dir_1) OR update(file_1, [dir_1, dir_2])
        """
        args_iter = itertools.chain(*((a, ) if isinstance(a, (str, Path)) else a for a in (arg, *args)))
        args_iter, peek = itertools.tee(args_iter)
        first = next(peek, None)
        if first is None:
            # No Path element in arg and args
            return self
        if Path(first).is_dir():
            result: Set[FileIndex] = self._update_multiple_directories(args_iter)
        else:
            result: Set[FileIndex] = self._update_multiple_files(args_iter)
        # # @Deprecated: Return Sequence[Path]
        # return tuple(self._get_file_info(file_index)[0] for file_index in result)
        return self

    def duplicates(self) -> Iterator[Sequence[Path]]:
        """
        Return the duplicates: [
            [file_A_1, file_A_2],
            [file_B_1, file_B_2, file_B_3],
            ...
        ]
        """
        for _, v in self.full_hash_dict.items():
            yield tuple(self._get_file_info(file_index)[0] for file_index in v)

    # simple api
    __call__ = update

    # short api name
    dup = duplicates


def print_duplicates(dirpath: Path, *dirpaths: Path) -> int:
    justone = JustOne()
    try:
        duplicates_list = justone((dirpath, *dirpaths)).dup() # equal to justone.update(...).duplicates()
    except JustOneError as e:
        print(f'Error: {format_exception_chain(e)}')
        return 1
    for duplicates in duplicates_list:
        print(f'Duplicate found:')
        for fp in duplicates:
            try:
                print(f' - {fp}')
            except UnicodeEncodeError:
                print(f' - {bytes(fp)}  [File Name Unicode Encode Error]')
        print(f'')
    return 0


def parse_args():
    def get_folder_path(path_string) -> Path:
        p = Path(path_string)
        if not p.is_dir():
            raise argparse.ArgumentTypeError(f'{path_string} is not a valid path for an existed folder.')
        return p

    parser = argparse.ArgumentParser(description='Fast duplicate files finder', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('directory', metavar='FOLDER', type=get_folder_path, nargs='+', help='文件夹路径')
    parser.add_argument('-s', '--strict', action='store_const', const=True, default=False, help='逐个字节对比，防止hash碰撞')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help='显示此命令行当前版本')

    args: argparse.Namespace = parser.parse_args()
    # if not args.directory and not args.image and not args.list:
    #     parser.print_help()
    # if args.list or args.image:
    #     # the sqlite db file needs to be existed.
    #     if not args.database.is_file():
    #         raise argparse.ArgumentTypeError(f'{args.database} is not existed.')
    # elif args.directory:
    #     # make sure the directory of sqlite db file is existed.
    #     args.database.parents[0].mkdir(parents=True, exist_ok=True)
    return args


def main() -> int:
    try:
        args: argparse.Namespace = parse_args()
    except argparse.ArgumentTypeError as e:
        print(f'命令行参数错误：{format_exception_chain(e)}')
        return 1
    if args.strict:
        print(f'此功能暂时未完成')
        return 1
    dirs: Final[Sequence[Path]] = args.directory
    return print_duplicates(*dirs)


if __name__ == '__main__':
    sys.exit(main())
