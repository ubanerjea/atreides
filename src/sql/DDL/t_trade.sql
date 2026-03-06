create table t_trade
(
    trade_id int generated always as identity primary key,
    parent_id int null,
    trade_type_id int not null,
    position_id int not null,
    status_id int not null,
    trade_date date not null default current_date,
    settle_date date not null default current_date,
    post_date date null,
    quantity int not null,
    price decimal(12, 2) not null default 1.00,
    rate decimal(10, 4) not null default 0.0000,
    create_ts timestamp not null default current_timestamp,
    create_user_id int not null default 1,
    comments varchar(4000) null,
    trade_ref varchar(255) null,
    v_01 varchar(255) null,
    v_02 varchar(255) null,
    v_03 varchar(255) null
);

insert into t_trade (trade_type_id, position_id, status_id, quantity, price) values (1, 1, 1, 1000, 1.00);

