/*****************************************************
*    @file :user_add_news_pre.cpp
*    @data :2014/12/24
*    @author :conan_dhn
*    @remark : ����ÿ�����ŵ�ƫ��,���������ݿ�
****************************************************/
#include<winsock2.h>
#include"mysql.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<iostream>
#include<sstream>
#include<algorithm>
#include<vector>
#include<string>
#include<set>
#include<map>

using namespace std;

MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //��msyql��ִ��order������ذ��������ָ��
{
    MYSQL_RES *result = nullptr;
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order1 error, %s!\n", mysql_error(mysqlPtr));
        exit(1);
    }
    else if ( (result = mysql_store_result(mysqlPtr)) == NULL )
    {
        fprintf(stderr, "save result error!\n");
        exit(1);
    }
    else
        return result;
}

void queryOrder2(MYSQL *mysqlPtr, char *order)              //��mysql��ִ�У������ؽ��������
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        //fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
}

void connector(char *sqlName, MYSQL *mysql)
{
	if (!mysql_real_connect(mysql, "localhost", "root", "dhn15158882311", sqlName, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(mysql));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql connect to user_name successfully!\n");
	}
}

void setName(MYSQL *mysql)
{
	if (mysql_query(mysql, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(mysql));
		exit(1);
	}
}
void setCharacter(MYSQL *mysql)
{
	if ( mysql_set_character_set( mysql , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(mysql));
	}
}
#define NDEBUG

int main()
{
	MYSQL mysql1;
	MYSQL mysql2;
    MYSQL mysql3;
    MYSQL mysql4;
	char sqlName1[20] = "user_name";
	char sqlName2[20] = "test20";
	char sqlName3[20] = "user_words_pre";
	mysql_init(&mysql1);	//initialize
	mysql_init(&mysql2);
	mysql_init(&mysql3);

    connector(sqlName1, &mysql1);
    connector(sqlName2, &mysql2);
    connector(sqlName3, &mysql3);

    setName(&mysql1);
    setName(&mysql2);
    setName(&mysql3);

    setCharacter(&mysql1);
	setCharacter(&mysql2);
	setCharacter(&mysql3);

	char getName[20];
	sprintf(getName, "select * from user_name.user_name");      //��user_name�л�ȡuser_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;

    //��ȡ����
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //ѭ��ÿ���û�A
    {
        //�������
        char add[50];
        sprintf(add, "alter table test20.%s add pre double default 0.0", userIdRow[0]);
        queryOrder2(&mysql2, add);

        //��ȡÿ���ִʼ���ƫ�ñ���Ϊmap
        map<string, double> word_pre;
        char getWordPre[50];
        sprintf(getWordPre, "select * from user_words_pre.%s", userIdRow[0]);
        MYSQL_RES *wordPreRes = queryOrder1(&mysql3, getWordPre);
        MYSQL_ROW wordPreRow;
        while ( (wordPreRow = mysql_fetch_row(wordPreRes)) != NULL )
            word_pre[ wordPreRow[0] ] = atof( wordPreRow[3] );

        //��ȡÿһ�����ţ���������ŵ�ƫ�ã����޸����ݿ�
        char getNews[50];
        sprintf(getNews, "select * from test20.%s", userIdRow[0]);
        MYSQL_RES *newsRes = queryOrder1(&mysql2, getNews);
        MYSQL_ROW newsRow;
        int i = 0;
        while ( (newsRow = mysql_fetch_row(newsRes)) != NULL )          //ȡÿһ������
        {
            i++;
            double curNewPre = 0.0;
            istringstream news(newsRow[2]);     //��ÿ������
            string word;
            while (news >> word)                                    //���㵱ǰ���ŵ�ƫ��
            {
                curNewPre += word_pre[word];
            }

            //���������ŵ�ƫ��д�����ݿ�
            char writePre[50];
            sprintf(writePre, "update test20.%s set pre=%f where id = %d", userIdRow[0], curNewPre, i);
            queryOrder2(&mysql2, writePre);
        }


    }


    mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}



