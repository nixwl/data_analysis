-- 添加 base_data 的自增主键
# ALTER TABLE base_data ADD 编号 int;
# ALTER TABLE base_data ADD PRIMARY KEY(编号);
# ALTER TABLE base_data CHANGE 编号 编号 INT NOT NULL AUTO_INCREMENT;

-- 注意：此处 SQL 文件在 首次 update 后需要执行一次，修改首次生成的表
-- 对 AaP_data 表的修改
#
# ALTER TABLE AaP_data ADD PRIMARY KEY(id);
# alter table spark.AaP_data
#     add constraint AaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
#
# ALTER TABLE TaP_data ADD PRIMARY KEY(id);
# alter table spark.TaP_data
#     add constraint TaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
# ALTER TABLE RaP_data ADD PRIMARY KEY(id);
# alter table spark.RaP_data
#     add constraint RaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
# ALTER TABLE MaP_data ADD PRIMARY KEY(id);
# alter table spark.MaP_data
#     add constraint MaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
# ALTER TABLE LaP_data ADD PRIMARY KEY(id);
# alter table spark.LaP_data
#     add constraint LaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
# ALTER TABLE BTaP_data ADD PRIMARY KEY(id);
# alter table spark.BTaP_data
#     add constraint BTaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
# ALTER TABLE CaPB_data ADD PRIMARY KEY(id);
# alter table spark.CaPB_data
#     add constraint CaPB_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
#
# ALTER TABLE CaT_data ADD PRIMARY KEY(id);
# alter table spark.CaT_data
#     add constraint CaT_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;
#
#
# ALTER TABLE CaP_data ADD PRIMARY KEY(id);
# alter table spark.CaP_data
#     add constraint CaP_data_base_data_id_fk
#         foreign key (id) references spark.base_data (id)
#             on update cascade;

update type_and_concern_analysis set factor = round(factor,2);
update type_analysis set rate = round(rate,2);
update TaP_datagrouped_analysis_6室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_6室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_5室3厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_5室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);

update TaP_datagrouped_analysis_5室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_4室3厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_4室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_4室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);

update TaP_datagrouped_analysis_3室3厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_3室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_3室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_2室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);

update TaP_datagrouped_analysis_2室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_2室0厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_1室2厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_1室1厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);
update TaP_datagrouped_analysis_1室0厅db set  room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2);

DELIMITER //
DROP PROCEDURE IF EXISTS `test`;
CREATE PROCEDURE test()
BEGIN
    DECLARE i INT;
    DECLARE table_name VARCHAR(50); -- 声明一个变量来存储表名
    SET i = 0;

    WHILE i < 10 DO
        SET table_name = CONCAT('RaP_datagrouped_analysis_', i, 'db');

        SET @sql_query = CONCAT(
            'UPDATE `', table_name, '` ',
            'SET room = ROUND(room, 2), room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2)'
        );

        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL test();

UPDATE MaP_datagrouped_analysis_其他db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);
UPDATE MaP_datagrouped_analysis_毛坯db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);
UPDATE MaP_datagrouped_analysis_精装db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);
UPDATE MaP_datagrouped_analysis_简装db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);

UPDATE LaP_datagrouped_analysis_中楼层db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);
UPDATE LaP_datagrouped_analysis_高楼层db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);
UPDATE LaP_datagrouped_analysis_低楼层db SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price,2);


update CaT_datagrouped_analysis_6室3厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_6室2厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_6室1厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_5室3厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_5室2厅db set  concern_rate = ROUND(concern_rate, 2);

update CaT_datagrouped_analysis_5室1厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_4室3厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_4室2厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_4室1厅db set  concern_rate = ROUND(concern_rate, 2);

update CaT_datagrouped_analysis_3室3厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_3室2厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_3室1厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_2室2厅db set  concern_rate = ROUND(concern_rate, 2);

update CaT_datagrouped_analysis_2室1厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_2室0厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_1室2厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_1室1厅db set  concern_rate = ROUND(concern_rate, 2);
update CaT_datagrouped_analysis_1室0厅db set  concern_rate = ROUND(concern_rate, 2);




update CaPB_datagrouped_analysis_西南db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_西北db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_西db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_南db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_东南db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_东北db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_东db set  concern_rate = ROUND(concern_rate, 2);
update CaPB_datagrouped_analysis_北db set  concern_rate = ROUND(concern_rate, 2);



DELIMITER //
DROP PROCEDURE IF EXISTS `test`;
CREATE PROCEDURE test()
BEGIN
    DECLARE i INT;
    DECLARE table_name VARCHAR(50); -- 声明一个变量来存储表名
    SET i = 0;

    WHILE i < 7 DO
        SET table_name = CONCAT('CaP_datagrouped_analysis_', i, 'db');

        SET @sql_query = CONCAT(
            'UPDATE `', table_name, '` ',
            'SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2)'
        );

        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL test();



DELIMITER //
DROP PROCEDURE IF EXISTS `test`;
CREATE PROCEDURE test()
BEGIN
    DECLARE i INT;
    DECLARE table_name VARCHAR(50); -- 声明一个变量来存储表名
    SET i = 0;

    WHILE i < 10 DO
        SET table_name = CONCAT('BTaP_datagrouped_analysis_', i, 'db');

        SET @sql_query = CONCAT(
            'UPDATE `', table_name, '` ',
            'SET built_time = ROUND(built_time, 2), room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2)'
        );

        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL test();


DELIMITER //
DROP PROCEDURE IF EXISTS `test`;
CREATE PROCEDURE test()
BEGIN
    DECLARE i INT;
    DECLARE table_name VARCHAR(50); -- 声明一个变量来存储表名
    SET i = 0;

    WHILE i < 10 DO
        SET table_name = CONCAT('AaP_datagrouped_analysis_', i, 'db');

        SET @sql_query = CONCAT(
            'UPDATE `', table_name, '` ',
            'SET room_price = ROUND(room_price, 2), total_price = ROUND(total_price, 2)'
        );

        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL test();


UPDATE address_mean SET mean_room_price = ROUND(mean_room_price, 2);
UPDATE built_time_analysis SET rate = ROUND(rate, 2);
UPDATE mod_analysis SET rate = ROUND(rate, 2);
UPDATE level_analysis SET rate = ROUND(rate, 2);

