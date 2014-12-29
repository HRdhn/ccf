/*****************************************************
*   @file   examine_result.cpp
*   @author HaoNan Dai
*   @date   2014/12/29
*   @remark examine the result
*           count the quantity of the users
*           we recommend successfully
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


using namespace std;



MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //在msyql上执行order命令，返回包含结果的指针
{
    MYSQL_RES *result = nullptr;
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order1 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
    else if ( (result = mysql_store_result(mysqlPtr)) == NULL )
    {
        fprintf(stderr, "save result error!\n");
        //exit(1);
    }
    else
        return result;
}

void queryOrder2(MYSQL *mysqlPtr, char *order)              //在mysql上执行，不返回结果的命令
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
}

void connector(char *sqlName, MYSQL *mysql)                 //连接数据库
{
	if (!mysql_real_connect(mysql, "localhost", "root", "dhn15158882311", sqlName, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(mysql));											//prototype   const char *mysql_error(MYSQL *mysql)
      //exit(1);
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
		//exit(1);
	}
}
void setCharacter(MYSQL *mysql)                                //设置字符集
{
	if ( mysql_set_character_set( mysql , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(mysql));
	}
}


int main()
{
	MYSQL mysql1;
	MYSQL mysql2;
    MYSQL mysql3;
    MYSQL mysql4;
	char sqlName1[20] = "reference";
	char sqlName2[20] = "user_rec_news";
	mysql_init(&mysql1);	//初始化
	mysql_init(&mysql2);

    connector(sqlName1, &mysql1);
    connector(sqlName2, &mysql2);

    setName(&mysql1);
    setName(&mysql2);

    setCharacter(&mysql1);
	setCharacter(&mysql2);

	char getName[20];
	sprintf(getName, "select * from reference.reference");
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;

    size_t counter = 0;
    int i = 0;

    //读取每个用户及其参考新闻
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL )
    {


        string userId = userIdRow[0];           //每个用户的id
        string reference_news = userIdRow[1];   //每个用户预期推荐的新闻

        char getRecNews[50];
        sprintf(getRecNews, "select * from user_rec_news.%s", userId.c_str());
        MYSQL_RES *recNewsRes = queryOrder1(&mysql2, getRecNews);
        MYSQL_ROW recNewsRow;

        vector<string> recNewsVec;
        while ( (recNewsRow = mysql_fetch_row(recNewsRes)) != NULL )        //读取为该用户推荐的五条新闻
        {
            recNewsVec.push_back(recNewsRow[0]);
        }

        auto findIter = find(recNewsVec.begin(), recNewsVec.end(), reference_news);
        if ( findIter != recNewsVec.end() )                                 //若为其推荐的五条新闻中有参考新闻，则成功一个用户
            counter++;
    }

    cout << "匹配用户数量: " << counter << endl;
    mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}


