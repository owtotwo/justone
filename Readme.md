# justone -- Fast find duplicate files in folder

`author: owtotwo`

## Usage

```
usage: justone.py [-h] [-s] [-i] [-t] [-v] FOLDER [FOLDER ...]

Fast duplicate files finder

positional arguments:
  FOLDER              文件夹路径

optional arguments:
  -h, --help          show this help message and exit
  -s, --strict        [0][default] 基于hash比较
                      [1][-s] 基于文件stat的shallow对比，不一致时进行字节对比，防止hash碰撞
                      [2][-ss] 严格逐个字节对比，防止文件stat与hash碰撞
  -i, --ignore-error  忽略权限、文件不存在等异常，继续执行（此时将忽略相应文件的重复可能）
  -t, --time          记录总用时消耗
  -v, --version       显示此命令行当前版本
```

## Requirements
`Fake Information as below...`
- Windows 10
- Python3.7+
- pip
- pypi
  + xxhash == 2.0.0 (Recommended, but not necessary)
  + tqdm == 4.49.0 (Not necessary)


## Install by pip and Run on Win10
```
$ pip install justone
$ justone -h
$ justone 'D:\data' 'C:\WeGame'
$ justone 'D:\fragmented files' -s
```

## License
[LGPLv3](./License) © [owtotwo](https://github.com/owtotwo)
