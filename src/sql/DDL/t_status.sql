create table t_status
(
    status_id int primary key,
    status_name varchar(255) not null,
    for_position boolean not null default false,
    for_trade boolean not null default false
);

-- select * from t_status order by 1;

insert into t_status (status_id, status_name, for_position, for_trade) values (1, 'Open', true, true);
insert into t_status (status_id, status_name, for_position, for_trade) values (2, 'Closed', true, false);
insert into t_status (status_id, status_name, for_position, for_trade) values (3, 'Future', true, true);
insert into t_status (status_id, status_name, for_position, for_trade) values (4, 'Cancelled', true, true);
insert into t_status (status_id, status_name, for_position, for_trade) values (5, 'Failed', true, true);
insert into t_status (status_id, status_name, for_position, for_trade) values (6, 'Preapproved', true, false);
insert into t_status (status_id, status_name, for_position, for_trade) values (7, 'Void', true, true);
