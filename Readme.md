# justone -- Fast find duplicate files in folder

`author: owtotwo`

## Usage

```
usage: justone [-h] [-s] [-v] FOLDER [FOLDER ...]

Fast duplicate files finder

positional arguments:
  FOLDER         文件夹路径

optional arguments:
  -h, --help     show this help message and exit
  -s, --strict   逐个字节对比，防止hash碰撞
  -v, --version  显示此命令行当前版本
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
```

## License
[LGPLv3](./License) © [owtotwo](https://github.com/owtotwo)
