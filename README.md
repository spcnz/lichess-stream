# Chess games analyzer

About
====
Project for course Big data architectures at FTN, Novi Sad. 
Project consists of two parts: 
- Stream processing
- Batch processing

Real-time processing includes
streaming chess moves from game currently played at [lichess tv game](https://lichess.org/tv) and analysing board state with [stockfish chess engine](https://stockfishchess.org/).


Batch processing includes storing large [dataset](https://www.kaggle.com/datasets/maca11/chess-games-from-lichess-20132014) to distributed file system HDFS
and processing it using Apache Spark. 

Milena Laketić
R2 22/ 2021

Prerequirements
====
Docker & docker-compose tool for managing containerized applications.


Architecture components
====
![big_daaaata_dij drawio](https://user-images.githubusercontent.com/52412989/170780804-6a71dacc-5a9e-4148-8e02-abeb5457fafa.png)
