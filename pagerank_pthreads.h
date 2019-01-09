/******************** Structs ********************/

/***** Структура для временных меток *****/
struct timeval start,end;

/***** Структура, используемая для данных потоков *****/

typedef struct
{
	int tid;
	int start, end;
} Thread; 

/***** Структура, используемая для данных узлов *****/

typedef struct
{
	double p_t0;
	double p_t1;
	double e;
	int *From_id;
	int con_size;
	int from_size;
}Node;
