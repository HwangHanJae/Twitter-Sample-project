# 트위터 API를 이용한 데이터 파이프라인
- 프로젝트의 더 자세한 내용은 [블로그](https://my-develop-note.tistory.com/233)에 포스팅하였습니다.

## 프로젝트 환경
프레임워크의 환경구성은 각 폴더에 저장하였습니다.
- VirtualBox의 가상머신(VM) 사용
- 가상머신(VM)은 `ubuntu-20.04` OS를 사용
- master 1개, slave 4개의 머신으로 구성
  - [master](master_node.bashrc) : CPU 프로세서 4개, RAM 8GB
  - [slave](slave_node.bashrc) : CPU 프로세서 4개, RAM 4GB
- [`hadoop-3.3.4`](/hadoop)
- [`spark-3.3.2`](/spark)
- [`hive-3.1.3`](/hive)
- [`presto-0.279`](/presto)
- [`airflow-2.5.2`](/airflow)
- `mongodb.4.4`
- `java-8`
- `python-3.8`

## 프로젝트 설명
트위터 API를 이용하여 같은 결과를 2가지 방법으로 데이터 파이프라인을 구축하였습니다.
- 가상머신(VM) 5개를 생성하고 ubuntu-20.04를 설치하였습니다.
- 고정 IP를 부여하고 ssh를 설정하여 서로 통신하게 만들었습니다.
- Hadoop을 설치하고 완전 분산 모드를 세팅하였습니다.  
- master VM에 mongodb.4.4를 설치하고 트위터 API를 이용하여 약 770만개의 데이터를 수집하였습니다.
  - [twitter_steaming.py](twitter_streaming.py)
  - 수집기간 : 2023-03-14 ~ 2023-03-21 
  - 데이터는 다음과 같이 구성됩니다.
    - lang : 작성자의 언어
    - created_at : 작성자가 트윗을 올린 날짜
    - text : 작성자가 트윗한 글의 내용
    - _timestamp : 현재 저장 시간
- spark 분산 환경 구축 및 hadoop yarn 위에서 spark 실행
  - hdfs를 spark-warehouse로 이용하여 spark에서 만들어진 테이블을 저장
- pyspark를 이용하여 데이터 파이프라인을 구축하였습니다.
  - ![image](https://user-images.githubusercontent.com/60374463/228698949-08ab847c-b265-481c-929c-8c0394aba1ce.png)
  - [spark_yarn.ipynb](spark_yarn.ipynb)
- airflow 설치 및 세팅
- airflow DAG 작성
  - TASK1 : embulk를 이용하여 MongoDB에서 데이터 추출 및 hdfs에 저장
    - [EXTRACT](airflow/dags/utils/etl.py)
  - TASK2 : hive를 이용하여 데이터 구조화 및 테이블 생성
    - [LOAD](airflow/dags/utils/etl.py)
  - TASK3 : presto를 이용하여 데이터 집계 및 csv 파일로 저장
    - [AGGREGATE](airflow/dags/utils/etl.py)
    
   
