/*****************************************************
    @data:2014/12/24
    @author:conan_dhn

    ��ȡ����
    for:�����û�
        �����û����ھӿ�������ѡ���Ե��Ƽ������û�
    д�����ݿ�

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
struct news                         //�������ű��⣬����ƫ��
{
    string new_id;
    double pre;
};

struct frd                          //���������ھӣ��������ƶ�
{
    string friend_id;
    double sim;
};

struct User_first                   //����ÿ���û������������ţ�����ƫ�ã������ھӣ�Ϊ���Ƽ������ű���
{
    vector<string> readed_news;             //���Ķ�������
    vector<string> recommended_news;        //Ϊ���Ƽ�������
    vector<frd> friends;                    //�����ھ�
    map<string, double> readed_news_pre;    //���Ķ������ŵ�ƫ��
};

map<string, User_first> all_user;                   //�û������û���Ϣ��ӳ��,�����û����Գ���ʱ��ӳ�䵽���û�����Ϣ

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
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        exit(1);
    }
}

void connector(char *sqlName, MYSQL *mysql)                 //�������ݿ�
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
void setCharacter(MYSQL *mysql)                                //�����ַ���
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
	sprintf(getName, "select * from user_name.user_name");      //��user_name�л�ȡuser_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;
    int i = 0;
    cout << "��ȡ����\n";
    //��ȡ����
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //ѭ��ÿ���û�A
    {
        if ( !(i % 50) )
            printf("%d\n", i);
        i++;

        User_first tem;
        //printf("%d, %s\n", i++, userIdRow[0]);


        //��ȡA�������ھӼ����ƶ�,���ݿ�user_friends�������
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

        //��ȡ���Ķ������ű��⣬���Ա����ƫ��,���ݿ�test20����Ԥ����
        char get_news[50];
        MYSQL_RES *newsRES;
        MYSQL_ROW newsRow;
        sprintf(get_news, "select * from test20.%s", userIdRow[0]);
        newsRES = queryOrder1(&mysql2, get_news);
        while ( (newsRow = mysql_fetch_row(newsRES)) != NULL )
        {
            tem.readed_news.push_back(newsRow[0]);                  //���Ķ�������
            tem.readed_news_pre[ newsRow[0] ] = atof(newsRow[4]);            //�Ը����ŵ�ƫ��
        }


        all_user[string(userIdRow[0])] = tem;                              //�����û���ӵ������û���

        #ifndef NDEBUG1
        User_first test = all_user.at(userIdRow[0]);
        cout << userIdRow[0] << ":\n";
        cout << "friends:\n";
        for (auto frIter = test.friends.begin(); frIter != test.friends.end(); ++frIter)    //����ھ�
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


    //Ϊÿ���û�A�Ƽ�����
    userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow2;
    cout << "��������\n";

    //��������
    int j = 0;
    while ( (userIdRow2 = mysql_fetch_row(userIdRes) ) != NULL)  //ѭ��ÿ���û�A
    {
        if ( !(j % 50) )
            printf("%d\n", j);
        j++;

        vector<news> alter_news;                                    //����ΪA�Ƽ�����������
        User_first &user_a = all_user.at(userIdRow2[0]);

        for (auto iter = user_a.friends.begin(); iter != user_a.friends.end(); ++iter)      //��ȡÿ�������ھ�
        {
            string user_bid = iter->friend_id;
            double sim = iter->sim;
            User_first user_b;
            try                                                         //�쳣����
            {
                user_b = all_user.at(user_bid);
            }
            catch (out_of_range err)
            {
                err.what();
                continue;
            }

            //��A���ھ�B��ѡ����ѡ����
            for (auto curNewIt = user_b.readed_news.begin(); curNewIt != user_b.readed_news.end(); ++curNewIt)
            {
                auto findOrNot = find(user_a.readed_news.begin(), user_a.readed_news.end(), *curNewIt);  //����B����������A�Ƿ��Ķ���
                if (findOrNot == user_a.readed_news.end())     //�ҵ�B������Aδ��������
                {
                    //����A�Ը������ŵ�ƫ��
                    double curNewpre = sim * user_b.readed_news_pre[*curNewIt];     //A,B�����ƶ� * B�Ը����ŵ�ƫ��
                    auto fdOrNot = find_if(alter_news.begin(), alter_news.end(), [curNewIt] (const news &t){return t.new_id == *curNewIt;});
                    if (fdOrNot == alter_news.end())                               //���������ű��浽�Ƽ��б���
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

        sort(alter_news.begin(), alter_news.end(), [] (const news &n1, const news &n2) {return n1.pre > n2.pre;});   //�Ժ�ѡ���Ű�A��ƫ������
        for (int i = 0; i < alter_news.size() && i < 5; i++)                                //ȡǰ�����Ƽ���A
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

    //д�����ݿ�
    cout << "д������\n";
    userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow3;
    int k = 0;
    while ( (userIdRow3 = mysql_fetch_row(userIdRes) ) != NULL)  //ѭ��ÿ���û�A
    {
        if (!(k % 50))
            printf("%d\n", k);
        k++;

        char insertNews[50];
//        char createTable[50];
//        sprintf( createTable, "create table user_rec_news.%s(recNews varchar (20))", userIdRow3[0] );
//        queryOrder1(&mysql4, createTable);          //Ϊÿ���û�����
        User_first curUser;
        try                                                     //�쳣����
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


