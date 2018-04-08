#__author:liwei
#date:2018/4/1
import os
import logging
logging.basicConfig(filename=os.path.join(os.path.dirname(__file__),'runLog/runLog'),
                    level=logging.INFO,format='runLog --->: %(asctime)s: %(levelname)s: %(message)s')

def setLogInfo(msg):
    logging.info(msg)

if __name__=='__main__':
    setLogInfo('12321')