create table data
(
    id integer primary key
);

alter table data
    owner to habrpguser;

insert into pgx_test.data (id) values (1),(2),(3);

alter user pgx_test with password 'qwerty';
grant all on schema pgx_test to pgx_test;