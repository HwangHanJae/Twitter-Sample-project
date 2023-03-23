import logging

def get_logger(name=None):
  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)
  #asctime : 시간정보
  #levelname : logging level
  #funcname : log가 기록된 함수
  #lineno : log가 기록된 line
  formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s"
  )
  console = logging.StreamHandler()
  file_handler = logging.FileHandler(filename='error.log')
  
  
  console.setLevel(logging.INFO)
  file_handler.setLevel(logging.INFO)

  console.setFormatter(formatter)
  file_handler.setFormatter(formatter)
  

  logger.addHandler(console)
  logger.addHandler(file_handler)


  return logger