#! encoding:utf-8

import platform

running_platform = platform.platform()
# 存放一些配置, 每次迁移工程的时候, 改这里就可以了
if running_platform.find('Windows') >= 0 or running_platform.find('windows') >= 0:
    # 在win上运行
    db_root_path = 'D:/share/info/'
    log_root_path = 'D:/share/record/'
    png_root_path = 'D:/share/png/'
else:
    # 在mac上运行
    db_root_path = '/Users/wgx/workspace/data/'
    log_root_path = '/Users/wgx/workspace/stonerecord/'
    png_root_path = '/Users/wgx/workspace/stoneresult/'


if __name__ == '__main__':
    print log_root_path

