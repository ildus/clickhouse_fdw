#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "clickhouse-client.h"


#define driver "/home/vagrant/percona/clickhouse-odbc/driver/libclickhouseodbc.so"

int select()
{
	int r;
	int deptno;
	const char *dname;
	const char *loc;
	int errno;
	char error[256];

	r = odbc_init();
	if (r < 0)
	{
		printf("\nERROR: unable to initalize");
		return -1;
	}

	Conn *conn = odbc_connect((char*)driver,(char*) "127.0.0.1", 8123, (char*)"default111", (char*)"", (char*)"", error);
	if (conn == 0)
	{
		printf("\nERROR: failed to connect\n %s\n", error);			
		return -1;
	}
	r = odbc_prepare(conn, (char*)"SELECT * FROM dept");
	if (r < 0)
	{
		printf("\nERROR: failed to prepare\n %s\n",conn->error);
		return -1;
	}
	r = odbc_execute(conn);
	if (r < 0)
	{
		printf("\nERROR: failed to execute\n %s\n", conn->error);
		return -1;
	}

	while(1)
	{
		char str[256];
		int len = 256;
		bool is_null = false;
		r = odbc_fetch(conn);
		if(r == 0)
			break;
		else if (r < 0)
		{		
			printf("\nERROR: failed to fetch\n %s\n", conn->error);
			break;
		}
		deptno = odbc_get_int(conn, 1, &errno);
		dname  = odbc_get_string(conn, 2, str, len, &is_null);
		loc    = odbc_get_string(conn, 3, str, len, &is_null);
		printf("\n%d, %s, %s\n", deptno, dname, loc);
	}
	r = odbc_disconnect(conn);
	if (r < 0)
		return -1;

	printf("\nall test passed\n");
	return 0;
}

int insert()
{
	int r;
	int deptno;
	const char *dname;
	const char *loc;
	int errno;
  char error[256];

	r = odbc_init();
	if (r < 0)
	{
		printf("\nERROR: unable to initalize");
		return -1;
	}

	Conn *conn = odbc_connect((char*)driver,(char*) "127.0.0.1", 8123, (char*)"default", (char*)"", (char*)"", error);
	if (conn == 0)
	{
		printf("\nERROR: failed to connect\n %s\n", error);			
		return -1;
	}

	r = odbc_prepare(conn, (char*)"INSERT INTO dept(deptno, dname, loc) VALUES(1, 2, 3)");
	if (r < 0)
	{
		printf("\nERROR: failed to prepare\n %s\n", conn->error);
		return -1;
	}
	if (odbc_bind_int(conn, 1, 100, false) < 0)  
	{
		printf("\nERROR: failed to bind deptno\n %s\n", conn->error);
		return -1;
	}
	if (odbc_bind_string(conn, 2, (char*)"100", false) < 0)  
	{
		printf("\nERROR: failed to bind dname\n %s\n", conn->error);
		return -1;
	}
	if (odbc_bind_string(conn, 3, (char*)"100", false) < 0)
	{
		printf("\nERROR: failed to bind loc\n %s\n", conn->error);
		return -1;
	}

	r = odbc_execute(conn);
	if (r < 0)
	{
		printf("\nERROR: failed to execute\n %s\n", conn->error);
		return -1;
	}
	r = odbc_disconnect(conn);
	if (r < 0)
		return -1;

	printf("\nall test passed\n");
	return 0;
}

int main()
{
	//insert();	
	select();
}
