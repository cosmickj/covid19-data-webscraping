# Covid19 Korea Webscraping
대한민국 코로나19 관련 현황 가져오기

> 결과 화면
> 
1. 코로나19 감염 관련 데이터
![Shine Covid19 Status UI](https://user-images.githubusercontent.com/59843639/116388371-b9a23000-a856-11eb-993d-f315843c0f79.png)

> 데이터베이스 테이블 설명

### 코로나19 국내 발생 현황(*covid19_domestic_region_num.py*)

<img width="100%" alt="covid19_domestic_num" src="https://user-images.githubusercontent.com/59843639/116644556-a6a27380-a9ae-11eb-927f-85f53a1f4f61.PNG">

- UPD_DATE: 데이터 업데이트 시간
- TOT_NUM: 코로나19 확진자 현황 누계
- TODAY(TOT): 금일 전체 코로나19 확진자 인원
- TODAY(DOM): 금일 국내 발생 코로나19 확진자 인원
- TODAY(INTL): 금일 해외 유입 코로나19 확진자 인원
- RLSE(TOT): 코로나19 격리해제 인원 누계
- RLSE(NEW): 금일 코로나19 격리해제 인원 증감
- QUAR(TOT): 코로나19 격리중인 인원 누계
- QUAR(NEW): 금일 코로나19 격리 인원 증감
- DTH(TOT): 코로나19 사망자 인원 누계
- DTH(NEW): 금일 코로나19 사망자 인원 증감

### 코로나19 시도별 발생 현황(*covid19_domestic_region_num.py*)

<img width="100%" alt="covid19_region_num" src="https://user-images.githubusercontent.com/59843639/116644577-b621bc80-a9ae-11eb-8d37-9376507229c4.PNG">

- UPD_DATE: 데이터 업데이트 시간
- SIDO: 각 시도 이름
- SIGUN: SIDO와 동일값
- TOT_NUM: 해당 시도 코로나19 확진자 현황 누계
- TODAY(TOT): 해당 시도 금일 전체 코로나19 확진자 인원
- TODAY(DOM): 해당 시도 금일 국내 발생 코로나19 확진자 인원
- TODAY(INTL): 해당 시도 금일 해외 유입 코로나19 확진자 인원
- RLSE(TOT): 해당 시도 코로나19 격리해제 인원 누계
- QUAR(TOT): 해당 시도 코로나19 격리중인 인원 누계
- DTH(TOT): 해당 시도 코로나19 사망자 인원 누계
- ~~RLSE(NEW): 금일 코로나19 격리해제 인원 증감 (어플 내에서 계산해서 사용)~~
- ~~QUAR(NEW): 금일 코로나19 격리 인원 증감 (어플 내에서 계산해서 사용)~~
- ~~DTH(NEW): 금일 코로나19 사망자 인원 증감 (어플 내에서 계산해서 사용)~~

### 코로나19 시군구별 발생 현황(*covid19_each_muni_version1.2.0.py*)

<img width="30%" alt="covid19_municipality_1" src="https://user-images.githubusercontent.com/59843639/116644587-bd48ca80-a9ae-11eb-93b9-0606c8bbe715.PNG"> <img width="30%" alt="covid19_municipality_2" src="https://user-images.githubusercontent.com/59843639/116644593-c20d7e80-a9ae-11eb-8f2b-0cc6f184ff83.PNG"> <img width="30%" alt="covid19_municipality_3" src="https://user-images.githubusercontent.com/59843639/116645081-fc2b5000-a9af-11eb-8308-2280bad2548a.PNG">


- SIDO: 각 시도 이름
- SIGUN: 각 시군구 이름
- UPD_DATE: 데이터 업데이트 기준 시간
- TOT_NUM: 해당 UPD_DATE 기준 누적 코로19 확진자 현황

### 코로나19 백신 접종 국내 현황(*covid19_vaccine_kr_version1.1.0.py*)

### 코로나19 백신 제조사별 접종 현황(*covid19_vaccine_cose.py*)

### 코로나19 백신 제조사별 이상반응 현황(*covid19_vaccine_cose.py*)
