'''
Created on 19-May-2018

@author: srinivasan
'''


class FileUtils:
    
    """
    Create the temp folder in local file system
    """

    @staticmethod
    def createTempFolder():
        import tempfile
        return tempfile.mkdtemp()

    """
    Delete the file/folder in local file system
    """

    @staticmethod
    def deletePath(dirpath):
        import shutil
        if FileUtils.isExist(dirpath):
            shutil.rmtree(dirpath)

    """
    check File exists
    """

    @staticmethod
    def isExist(file_path): 
        import os.path
        return  os.path.exists(file_path)   

    """
    Create Directory
    """

    @staticmethod
    def createDir(directory):
        import os, errno
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
