
import os
# import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
from ES_Bulk_Incre_Project.Lib.Logging.Logging import *
import datetime

def os_walk_searchdir(dirname):
    root_dir = dirname
    sub_dir = []
    for (root, dirs, files) in os.walk(root_dir):
        log.info("# root : " + root)
        if len(dirs) > 0:
            for dir_name in dirs:
                log.info("dir: " + dir_name)
                sub_dir.append(root+dir_name)
        else:
            log.info('# out')
            if len(sub_dir) < 1:
                sub_dir.append(root[:len(root)-1])

    log.info("sub_dir: " + ''.join(sub_dir))

    return sub_dir

def search_subdir(dirname):
    sub_dir = []
    filenames = os.listdir(dirname)
    for filename in filenames:
        full_filename = os.path.join(dirname, filename)
        sub_dir.append(full_filename)
        # print (full_filename)
    return sub_dir


def get_all_files(rootDirectory):
    """
    특정 디렉토리(root) 아래 전체 파일 리스트를 읽어옴 (os.walk)
    특정 파일 리스트를 list에 append시킴 (파일리스트는 색인 가능한 json 값이어야 함)
    :param rootDirectory:
    :return:
    """
    file_list = []
    try:
        # print(rootDirectory)
        if os.path.isdir(rootDirectory):
            for path, dir, files in os.walk(rootDirectory):
                dir.sort()
                for filename in files:
                    ext = os.path.splitext(filename)[-1]
                    file_list.append("%s/%s" % (path, filename))
                    print('\n\n',"%s/%s" % (path, filename))
                    '''
                    if ext == '.py':
                        print("%s/%s" % (path, filename))
                    '''
        else:
            print('\n\n')
            print('#'*50)
            print('No Directory')
            print('#'*50)
            print('\n\n')
    except Exception as ex:  #
        print('에러가 발생 했습니다', ex)

    return file_list