/*****************************************************
    @data:2014/12/24
    @author:conan_dhn

    读取数据
    for:所有用户
        将该用户的邻居看的新闻选择性的推荐给该用户
    写入数据库

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
#include<stdexcept>

using namespace std;
struct news                         //保存新闻标题，及其偏好
{
    string new_id;
    double pre;
};

struct frd                          //保存相似邻居，及其相似度
{
    string friend_id;
    double sim;
};

struct User_first                   //保存每个用户自身看过的新闻，及其偏好，相似邻居，为其推荐的新闻标题
{
    vector<string> readed_news;             //已阅读的新闻
    vector<string> recommended_news;        //为其推荐的新闻
    vector<frd> friends;                    //相似邻居
    map<string, double> readed_news_pre;    //已阅读的新闻的偏好
};

map<string, User_first> all_user;                   //用户名到用户信息的映射,允许用户名以常数时间映射到该用户的信息

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

void connector(char *sqlName, MYSQL *mysql)                 //连接数据库
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
void setCharacter(MYSQL *mysql)                                //设置字符集
{
	if ( mysql_set_character_set( mysql , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(mysql));
	}
}

#define NDEBUG1
#define NDEBUG2
#define NDEBUG3

int main()
{
	MYSQL mysql1;
	MYSQL mysql2;
    MYSQL mysql3;
    MYSQL mysql4;
	char sqlName1[20] = "user_name";
	char sqlName2[20] = "test20";
	char sqlName3[20] = "user_friends";
	char sqlName4[20] = "user_rec_news";
	mysql_init(&mysql1);
	mysql_init(&mysql2);
	mysql_init(&mysql3);
	mysql_init(&mysql4);

    connector(sqlName1, &mysql1);
    connector(sqlName2, &mysql2);
    connector(sqlName3, &mysql3);
    connector(sqlName4, &mysql4);

    setName(&mysql1);
    setName(&mysql2);
    setName(&mysql3);
    setName(&mysql4);

    setCharacter(&mysql1);
	setCharacter(&mysql2);
	setCharacter(&mysql3);
	setCharacter(&mysql4);

	int counter = 0;
	char getName[20];
	sprintf(getName, "select * from user_name.user_name");      //从user_name中获取user_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;
    int i = 0;
    cout << "读取数据\n";
    //读取数据
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //循环每个用户A
    {
        if ( !(i % 50) )
            printf("%d\n", i);
        i++;

        User_first tem;
        //printf("%d, %s\n", i++, userIdRow[0]);


        //读取A的相似邻居及相似度,数据库user_friends，需更新
        char get_friend[50];
        MYSQL_RES *friendsRes;
        MYSQL_ROW friendsRow;
        sprintf(get_friend, "select * from user_friends.%s", userIdRow[0]);
        friendsRes = queryOrder1(&mysql3, get_friend);
        int j = 0;
        while ( (friendsRow = mysql_fetch_row(friendsRes)) != NULL )
        {

            frd f;
            //printf("%s\n", friendsRow[0]);
            if (friendsRow[0] != NULL)
            {
                f.friend_id = friendsRow[0];
                f.sim = atof(friendsRow[1]);
                tem.friends.push_back(f);
            }

        }

        //读取已阅读的新闻标题，及对标题的偏好,数据库test20需先预处理
        char get_news[50];
        MYSQL_RES *newsRES;
        MYSQL_ROW newsRow;
        sprintf(get_news, "select * from test20.%s", userIdRow[0]);
        newsRES = queryOrder1(&mysql2, get_news);
        while ( (newsRow = mysql_fetch_row(newsRES)) != NULL )
        {
            tem.readed_news.push_back(newsRow[0]);                  //已阅读的新闻
            tem.readed_news_pre[ newsRow[0] ] = atof(newsRow[4]);            //对该新闻的偏好
        }


        all_user[string(userIdRow[0])] = tem;                              //将该用户添加到所有用户中

        #ifndef NDEBUG1
        User_first test = all_user.at(userIdRow[0]);
        cout << userIdRow[0] << ":\n";
        cout << "friends:\n";
        for (auto frIter = test.friends.begin(); frIter != test.friends.end(); ++frIter)    //输出邻居
        {
            cout << frIter->friend_id << ": " << frIter->sim << endl;
        }
        cout << "readed news:\n";
        for (auto newIter = test.readed_news.begin(); newIter != test.readed_news.end(); ++newIter)
        {
            cout << *newIter << ": " << test.readed_news_pre[*newIter] << endl;
        }
        system("pause");
        #endif // NDEBUG1
    }


    //为每个用户A推荐新闻
    userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow2;
    cout << "处理数据\n";

    //处理数据
    int j = 0;
    while ( (userIdRow2 = mysql_fetch_row(userIdRes) ) != NULL)  //循环每个用户A
    {
        if ( !(j % 50) )
            printf("%d\n", j);
        j++;

        vector<news> alter_news;                                    //保存为A推荐的所有新闻
        User_first &user_a = all_user.at(userIdRow2[0]);

        for (auto iter = user_a.friends.begin(); iter != user_a.friends.end(); ++iter)      //读取每个相似邻居
        {
            string user_bid = iter->friend_id;
            double sim = iter->sim;
            User_first user_b;
            try                                                         //异常处理
            {
                user_b = all_user.at(user_bid);
            }
            catch (out_of_range err)
            {
                err.what();
                continue;
            }

            //从A的邻居B中选出备选新闻
            for (auto curNewIt = user_b.readed_news.begin(); curNewIt != user_b.readed_news.end(); ++curNewIt)
            {
                auto findOrNot = find(user_a.readed_news.begin(), user_a.readed_news.end(), *curNewIt);  //检验B读过的新闻A是否阅读过
                if (findOrNot == user_a.readed_news.end())     //找到B读过而A未读的新闻
                {
                    //计算A对该条新闻的偏好
                    double curNewpre = sim * user_b.readed_news_pre[*curNewIt];     //A,B的相似度 * B对该新闻的偏好
                    auto fdOrNot = find_if(alter_news.begin(), alter_news.end(), [curNewIt] (const news &t){return t.new_id == *curNewIt;});
                    if (fdOrNot == alter_news.end())                               //将该条新闻保存到推荐列表中
                    {
                        news tem;
                        tem.new_id = *curNewIt;
                        tem.pre = curNewpre;
                        alter_news.push_back(tem);
                    }//end if
                    else
                        fdOrNot->pre += curNewpre;
                }//end if
            }//end for

        }//end for

        sort(alter_news.begin(), alter_news.end(), [] (const news &n1, const news &n2) {return n1.pre > n2.pre;});   //对候选新闻按A的偏好排序
        for (int i = 0; i < alter_news.size() && i < 5; i++)                                //取前五条推荐给A
        {
            user_a.recommended_news.push_back(alter_news[i].new_id);
        }



        #ifndef  NDEBUG2
        cout << userIdRow2[0] << ":\n";
        for (auto recNewiter = user_a.recommended_news.begin(); recNewiter != user_a.recommended_news.end(); ++recNewiter)
            cout << *recNewiter << endl;
        system("pause");

        #endif // NDEBUG2


    }//end while

    #ifndef NDEBUG3
    cout << "the size of all_user:" << all_user.size() << endl;

    system("pause");
    for (auto userIter = all_user.begin(); userIter != all_user.end(); ++userIter)
    {
        auto second = userIter->second;
        for (auto newIter = second.recommended_news.begin(); newIter != second.recommended_news.end(); ++newIter)
        {
            printf("%s\n", (*newIter).c_str());
        }
        cout << endl;
        system("pause");
    }
    #endif // NDEBUG3

    //写入数据库
    cout << "写入数据\n";
    userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow3;
    int k = 0;
    while ( (userIdRow3 = mysql_fetch_row(userIdRes) ) != NULL)  //循环每个用户A
    {
        if (!(k % 50))
            printf("%d\n", k);
        k++;

        char insertNews[50];
//        char createTable[50];
//        sprintf( createTable, "create table user_rec_news.%s(recNews varchar (20))", userIdRow3[0] );
//        queryOrder1(&mysql4, createTable);          //为每个用户建表
        User_first curUser;
        try                                                     //异常处理
        {
            curUser = all_user.at(userIdRow3[0]);
        }
        catch (out_of_range err)
        {
            err.what();
            continue;
        }

        for (auto recIer = curUser.recommended_news.begin(); recIer != curUser.recommended_news.end(); ++recIer)
        {
            sprintf(insertNews, "insert into user_rec_news.%s values (%s)", userIdRow3[0], (*recIer).c_str());
            queryOrder2(&mysql4, insertNews);
        }
    }
    cout << counter << endl;
    mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}


