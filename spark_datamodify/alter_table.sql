-- base_data 的建表语句
# CREATE TABLE `base_data` (
#   `编号` int(11) NOT NULL AUTO_INCREMENT,
#   `名称` text NOT NULL,
#   `地址` text NOT NULL,
#   `房型` text NOT NULL,
#   `面积` double NOT NULL,
#   `朝向` text NOT NULL,
#   `装修` text NOT NULL,
#   `楼层` text NOT NULL,
#   `建成时间` text NOT NULL,
#   `楼层结构` text NOT NULL,
#   `总价格` double NOT NULL,
#   `平方价格` double NOT NULL,
#   `关注度` int(11) NOT NULL,
#   `发布时间` text NOT NULL,
#   PRIMARY KEY (`编号`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 添加 base_data 的自增主键
# ALTER TABLE base_data ADD 编号 int;
# ALTER TABLE base_data ADD PRIMARY KEY(编号);
# ALTER TABLE base_data CHANGE 编号 编号 INT NOT NULL AUTO_INCREMENT;

-- 注意：此处 SQL 文件在 首次 update 后需要执行一次，修改首次生成的表
-- 对 AaP_data 表的修改

ALTER TABLE AaP_data ADD PRIMARY KEY(id);
alter table spark.AaP_data
    add constraint AaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;


ALTER TABLE TaP_data ADD PRIMARY KEY(id);
alter table spark.TaP_data
    add constraint TaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;

ALTER TABLE RaP_data ADD PRIMARY KEY(id);
alter table spark.RaP_data
    add constraint RaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;

ALTER TABLE MaP_data ADD PRIMARY KEY(id);
alter table spark.MaP_data
    add constraint MaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;

ALTER TABLE LaP_data ADD PRIMARY KEY(id);
alter table spark.LaP_data
    add constraint LaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;

ALTER TABLE BTaP_data ADD PRIMARY KEY(id);
alter table spark.BTaP_data
    add constraint BTaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;

ALTER TABLE CaPB_data ADD PRIMARY KEY(id);
alter table spark.CaPB_data
    add constraint CaPB_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;


ALTER TABLE CaT_data ADD PRIMARY KEY(id);
alter table spark.CaT_data
    add constraint CaT_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;


ALTER TABLE CaP_data ADD PRIMARY KEY(id);
alter table spark.CaP_data
    add constraint CaP_data_base_data_id_fk
        foreign key (id) references spark.base_data (id)
            on update cascade;


