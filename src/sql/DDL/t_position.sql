create table t_position
(
    position_id int generated always as identity primary key,
    pool_position_id int null,
    position_type_id int not null,
    status_id int not null,
    position_date date not null default current_date,
    settle_date date not null default current_date,
    quantity int not null,
    price decimal(12, 2) not null default 1.00,
    rate decimal(10, 4) null,
    create_ts timestamp not null default current_timestamp,
    create_user_id int not null default 1,
    position_ref varchar(255) null,
    comments varchar(4000) null,
    v_01 varchar(255) null,
    v_02 varchar(255) null,
    v_03 varchar(255) null
);

CREATE OR REPLACE FUNCTION copy_position_id_to_pool()
RETURNS TRIGGER AS $$
BEGIN
    -- Only copy if reference_id was not explicitly provided
    IF NEW.pool_position_id IS NULL THEN
        NEW.pool_position_id := NEW.position_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_copy_position_id_to_pool
BEFORE INSERT ON t_position FOR EACH ROW
EXECUTE FUNCTION copy_position_id_to_pool();


--insert into t_position (position_type_id, status_id, quantity, price) values (2, 1, 1000, 1.00);
--insert into t_position (pool_position_id, position_type_id, status_id, quantity, price) values (1, 2, 1, 1000, 1.00);
