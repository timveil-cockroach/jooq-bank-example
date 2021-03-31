drop table if exists accounts;

create table accounts (
  id uuid PRIMARY KEY,
  balance bigint not null,
);