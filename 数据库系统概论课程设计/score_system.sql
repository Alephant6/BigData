/*==============================================================*/
/* DBMS name:      MySQL 5.0                                    */
/* Created on:     2021/12/31 14:25:03                          */
/*==============================================================*/


drop table if exists course;

drop table if exists score;

drop table if exists student;

drop table if exists teacher;

/*==============================================================*/
/* Table: course                                                */
/*==============================================================*/
create table course
(
   cno                  int not null,
   wno                  int,
   cname                varchar(50) not null,
   point                int,
   classes              int,
   primary key (cno)
);

/*==============================================================*/
/* Table: score                                                 */
/*==============================================================*/
create table score
(
   sno                  int not null,
   cno                  int not null,
   score                int,
   examdate             datetime,
   primary key (sno, cno)
);

/*==============================================================*/
/* Table: student                                               */
/*==============================================================*/
create table student
(
   sno                  int not null,
   name                 varchar(50),
   address              varchar(50),
   tel                  varchar(50),
   primary key (sno)
);

/*==============================================================*/
/* Table: teacher                                               */
/*==============================================================*/
create table teacher
(
   wno                  int not null,
   name                 varchar(50),
   address              varchar(50),
   tel                  varchar(50),
   years                int,
   primary key (wno)
);

alter table course add constraint FK_Relationship_1 foreign key (wno)
      references teacher (wno) on delete restrict on update restrict;

alter table score add constraint FK_score foreign key (sno)
      references student (sno) on delete restrict on update restrict;

alter table score add constraint FK_score2 foreign key (cno)
      references course (cno) on delete restrict on update restrict;

