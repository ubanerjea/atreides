create table t_tradetype
(
    trade_type_id int primary key,
    trade_type_name varchar(255) not null,
    position_group varchar(255) null,
    is_origination_trade boolean not null default false,
    is_termination_trade boolean not null default false,
    quantity_change int not null default 0,
    price_change int not null default 0,
    is_rerate_trade boolean not null default false
);

-- select * from t_tradetype order by 1;

insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_origination_trade, quantity_change) values (1, 'New Loan', 'Loan', true, 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (2, 'Add Loan', 'Loan', 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (3, 'Return Loan', 'Loan', -1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_termination_trade, quantity_change) values (4, 'Return Full Loan', 'Loan', true, -1);

insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_origination_trade, quantity_change) values (11, 'New Collateral', 'Collateral', true, 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (12, 'Add Collateral', 'Collateral', 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (13, 'Return Collateral', 'Collateral', -1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_termination_trade, quantity_change) values (14, 'Return Full Collateral', 'Collateral', true, -1);

insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_origination_trade, quantity_change) values (21, 'New Borrow', 'Borrow', true, 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (22, 'Add Borrow', 'Borrow', 1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, quantity_change) values (23, 'Return Borrow', 'Borrow', -1);
insert into t_tradetype (trade_type_id, trade_type_name, position_group, is_termination_trade, quantity_change) values (24, 'Return Full Borrow', 'Borrow', true, -1);