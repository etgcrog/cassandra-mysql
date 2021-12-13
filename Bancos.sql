-- Criação da database e tabela no Mysql
create database oldtech;
use oldtech;

CREATE TABLE IF NOT EXISTS vendas(
nota_fiscal int primary key,
vendedor varchar(50),
total numeric(10,2)
);

select * from vendas;

-- Criação do Keyspace e tabela no Cassandra
CREATE KEYSPACE IF NOT EXISTS oldtech WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 1};

create table if not exists oldtech.vendas(
id uuid PRIMARY KEY,
vendedor text,
total decimal
);