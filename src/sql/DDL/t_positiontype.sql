create table t_positiontype
(
    position_type_id int primary key,
    position_type_name varchar(255) not null,
    parent_position_type_id int null,
    is_vs_cash boolean not null default false,
    is_cash boolean not null default false,
    is_vs_collateral boolean not null default false,
    is_collateral boolean not null default false,
    rate_base_type varchar(50) null,
    position_group varchar(255) null
);

-- select * from t_positiontype order by 1;

insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group) values (1, 'Loan', null, 'SFP');
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_vs_cash) values (2, 'Cash Loan', 1, 'Loan', true);
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_vs_collateral) values (3, 'NonCash Loan', 1, 'Loan', true);


insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group) values (10, 'Collateral', null, 'Collateral');
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_collateral) values (11, 'Collateral Receive', 10, 'Collateral', true);
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_collateral) values (12, 'Collateral Deliver', 10, 'Collateral', true);

insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group) values (20, 'Borrow', null, 'SFP');
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_vs_cash) values (21, 'Cash Borrow', 20, 'Borrow', true);
insert into t_positiontype (position_type_id, position_type_name, parent_position_type_id, position_group, is_vs_collateral) values (22, 'NonCash Borrow', 20, 'Borrow', true);

