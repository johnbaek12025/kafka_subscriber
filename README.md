# Analsis Data Manager
프로그램 설명 필요!!!

## ADM 실행 순서

1. git 설치
```
https://git-scm.com/
```

2. adm 프로그램 설치
```
git clone https://gitlab.com/tp.rc.cyg/kafka_subscriber.git
```

3. Python 3.8 설치
```
https://www.python.org/downloads/
```

4. 라이브러리 설치
- 파이썬 라이브러리
```
pip install -r requirements.txt
```

- 오라클 라이브러리
아래 링크에서 다운로드 받아 설치
```
https://www.oracle.com/kr/database/technologies/instant-client/winx64-64-downloads.html
```

5. ksm/examples 에 있는 ksm.cfg 를 수정하여서 특정 폴더에 저장
```
Ex) ksm/cfg/ksm.cfg
```

6. Unit-test 를 실행하여 문제 없는지 확인 (테스트 작성을 안했기 때문에, 패스)
```
Ex) tox
Ex) python setup.py test
Ex) python setup.py test --test-suite tests.test_util.TestUtil
Ex) python setup.py test --test-suite tests.test_util.TestUtil.test_to_int

```

7. 실행
```
# 옵션 파일이 ksm/cfg/에 위치할 경우
cd bin
python ks-manager.py --config=../cfg/cm.cfg
```


## 카프카 (정리중...)

```
zookeeper-server-start.bat D:\kafka\config\zookeeper.properties

kafka-server-start.bat D:\kafka\config\server.properties

kafka-topics.bat --list --bootstrap-server localhost:9092 

kafka-topics.bat --create --topic %1 --replication-factor 1 --partitions 1  --zookeeper localhost:2181

# server.properties 에서 delete.topic.enable = True 로 설정해도 안됨..
kafka-topics.bat --delete --zookeeper localhost:9092 --topic topic_name


kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test

kafka-console-producer.bat --bootstrap-server localhost:9092 --topic test





[server.properties]

advertised.listeners=PLAINTEXT://121.254.150.120:9092
```