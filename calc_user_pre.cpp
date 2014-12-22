/*****************************************
*计算用户对每个分词的偏好
*****************************************/

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

using namespace std;
double preFade(double fre)
{
    return fre;
}

double timeFade(char *viewdate)     //timeFade function
{
    char t[3];
    t[0] = viewdate[8];
    t[1] = viewdate[9];
    t[2] = '\0';
    double i = atof(t);
    return 1.0 / (32.0 - i);
}

MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //在msyql上执行order命令，返回包含结果的指针
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

void queryOrder2(MYSQL *mysqlPtr, char *order)              //在mysql上执行，不返回结果的命令
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        exit(1);
    }
}

int main1()
{
	MYSQL mysql1;
	MYSQL mysql2;
    MYSQL mysql3;
	MYSQL_RES *result = NULL, *titleRes = NULL;	//save result
	MYSQL_ROW row, titleRow;
	char sqlName1[20] = "user_name";
	char sqlName2[20] = "test20";
	char sqlName3[20] = "user_words_pre";
	mysql_init(&mysql1);	//initialize
	mysql_init(&mysql2);
	mysql_init(&mysql3);

    struct word  //保存每个分词信息
    {
        string wd;
        int times = 1;
        double frequency = 0.0;
        double preference = 0.0;
    };



	if (!mysql_real_connect(&mysql1, "localhost", "root", "dhn15158882311", sqlName1, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(&mysql1));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql1 connect to user_name successfully!\n");
	}
	if (!mysql_real_connect(&mysql2, "localhost", "root", "dhn15158882311", sqlName2, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(&mysql2));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql2 connect to test20 successfully!\n");
	}
	if (!mysql_real_connect(&mysql3, "localhost", "root", "dhn15158882311", sqlName3, 3306, NULL, 0))	//connect to test2011cp
	{
		fprintf(stderr, "%s\n", mysql_error(&mysql3));											//prototype   const char *mysql_error(MYSQL *mysql)
		exit(1);
	}
	else
	{
		fprintf(stderr, "mysql3 connect to user_word_pre successfully!\n");
	}

	if (mysql_query(&mysql1, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}
	if (mysql_query(&mysql2, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}
	if (mysql_query(&mysql3, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}

	if ( mysql_set_character_set( &mysql1 , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}
	if (mysql_set_character_set(&mysql2, "gbk"))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}
	if (mysql_set_character_set(&mysql3, "gbk"))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}


	int counter = 0;
	char getName[20];
	sprintf(getName, "select * from user_name.user_name");      //从user_name中获取user_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;

    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //循环每个用户id
    {
        //cout << userIdRow[0] << endl;
        char select[20];
        sprintf(select, "select * from test20.%s", userIdRow[0]);
        MYSQL_RES *user_new_res = queryOrder1(&mysql2, select);       //获取用户的每条新闻
        MYSQL_ROW user_new_row;
        vector<word> user_words;    //保存每个用户的分词信息


        //为每个用户建表
        char createTable[50];
        sprintf(createTable, "create table user_words_pre.%s(word varchar(20), times int default 0, frequency double default 0, preferency double default 0)", userIdRow[0]);
        queryOrder2(&mysql3, createTable);


        while ((user_new_row = mysql_fetch_row(user_new_res)) != NULL) //处理每条新闻
        {
            double timeFactor = timeFade(user_new_row[1]);      //计算每条新闻的时间因子
            word tem;
            istringstream user_new_rec(user_new_row[2]);

            while (user_new_rec >> tem.wd)
            {
                tem.frequency = timeFactor;
                auto iter = find_if(user_words.begin(), user_words.end(),
                                    [tem] (const word &w) {return tem.wd == w.wd;}); //寻找第一个分词
                if (iter == user_words.end())
                {
                    user_words.push_back(tem);
                }
                else
                {
                    iter->times++;
                    iter->frequency += tem.frequency;
                }
            }//while
        }//while

        char insert[50];
        for (auto iter = user_words.begin(); iter != user_words.end(); ++iter)     //计算每个分词的偏好,插入user_words_pre中
        {
            iter->preference = preFade(iter->frequency);
            sprintf(insert, "insert into user_words_pre.%s values ('%s', %d, %f, %f)", userIdRow[0], (iter->wd).c_str(), iter->times, iter->frequency, iter->preference);
            //system("pause");
            queryOrder2(&mysql3, insert);
            //cout << iter->wd << ' ' << iter->times << ' ' << iter->frequency << ' ' << iter->preference << endl;
        }
        //system("pause");

        counter++;
    }//while

    cout << counter << endl;

	mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}

