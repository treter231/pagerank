/********************************************************************/
/*    Pagerank project 2014 - Parallel version                      */
/*    	*based on Cleve Moler's matlab implementation               */
/*                                                                  */
/*    Implemented by Nikos Katirtzis (nikos912000)                  */
/********************************************************************/

/******************** Includes - Defines ****************/
#include "pagerank_pthreads.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>

#define maxThreads 64

/******************** Defines ****************/
// число узлов
int N, num_threads;

// Границы и параметр сходимости алгоритма d  
double threshold, d;

//Таблица потоков
pthread_t *Threads;

// Таблица с данными потоков
Thread *Threads_data;

// Таблица данных узла
Node *Nodes;
pthread_mutex_t lockP = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t locksum = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lockmax = PTHREAD_MUTEX_INITIALIZER;

// Количество потоков
int num_threads;

// Количество итераций
int iterations = 0;

double max_error = 1;
double sum = 0;

/***** Распределение памяти - инициализация для потоков *****/

void Threads_Allocation()
{

	int i;
	double N_split =  (double) N / num_threads;
	
	// Выделение памяти для потоков
	Threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));

	// Хранит данные дерева	
	Threads_data = (Thread*)malloc(num_threads * sizeof(Thread));	
	
	// Разбить набор данных на подмножества, заданные для каждого потока
	Threads_data[0].tid = 0;
	Threads_data[0].start = 0;
	Threads_data[0].end = floor(N_split);

	for (i = 1; i < num_threads; i++)
	{
		Threads_data[i].tid = i;
		Threads_data[i].start = Threads_data[i - 1].end;
		if (i < (num_threads - 1))
		{
			Threads_data[i].end = Threads_data[i].start + floor(N_split);
		}
		else
		{
			Threads_data[i].end = N;
		}
	}
	
	printf("\n");

	for (i = 0; i < num_threads; i++)
	{
		printf("Thread %d, start = %d, end = %d\n", Threads_data[i].tid, Threads_data[i].start, Threads_data[i].end);
	}

	printf("\n");

}

/***** Выделение памяти - инициализация узлов *****/
void Nodes_Allocation()
{

	int i;
	Nodes = (Node*)malloc(N*sizeof(Node));
    
    for (i = 0; i < N; i++)
	{
		Nodes[i].con_size = 0;
		Nodes[i].from_size = 0;
        Nodes[i].From_id = (int*) malloc(sizeof(int));
    }	

}

/***** Чтение соединений графа из txt-файла *****/	

void Read_from_txt_file(char* filename)
{
    
    FILE *fid;

    int from_idx, to_idx;
	int temp_size;
	char line[1000];

    fid = fopen("web-Google.txt", "r");
   	if (fid == NULL){printf("Error opening the file\n");}

	while (!feof(fid))
	{
		fgets(line, sizeof(line), fid);
		// игнорировать предложения, начинающиеся с #
		if (sscanf(line,"%d\t%d\n", &from_idx, &to_idx))
		{
			Nodes[from_idx].con_size++;
			Nodes[to_idx].from_size++;
			temp_size = Nodes[to_idx].from_size;
			Nodes[to_idx].From_id = (int*) realloc(Nodes[to_idx].From_id, temp_size * sizeof(int));
			Nodes[to_idx].From_id[temp_size - 1] = from_idx; 
		}
	}

	printf("End of connections insertion!\n");

	fclose(fid);

}

/***** Чтение вектора Р из txt файла*****/	

void Read_P_from_txt_file()
{

	FILE *fid;
	double temp_P;
	int index = 0;

    fid = fopen("P.txt", "r");
   	if (fid == NULL){printf("Error opening the Probabilities file\n");}

	while (!feof(fid))
	{
		// P's values are double!
		if (fscanf(fid,"%lf\n", &temp_P))
		{
			Nodes[index].p_t1 = temp_P;
			index++;	   
		}
	}
	printf("End of P insertion!");

	fclose(fid);	

}


/***** Чтение вектора E из txt файла*****/	

void Read_E_from_txt_file()
{

	FILE *fid;
	double temp_E;
	int index = 0;
	
    fid = fopen("E.txt", "r");
   	if (fid == NULL){printf("Error opening the E file\n");}

	while (!feof(fid))
	{
		// Значения вектора Е два раза!
		if (fscanf(fid,"%lf\n", &temp_E))
		{
			Nodes[index].e = temp_E;
			index++;   
		}
	}
	printf("End of E insertion!");

	fclose(fid);	

}

/***** Создать P и E с равной вероятностью *****/

void Random_P_E()
{

   	int i;
    // Сумма ветора P (должно быть =1)
    double sum_P_1 = 0;
    // Сумма ветора E (должно быть  =1)
    double sum_E_1 = 0; 
    
    
    // Инициализация массивов
    for (i = 0; i < N; i++)
    {
        Nodes[i].p_t0 = 0;
        Nodes[i].p_t1 = 1;
        Nodes[i].p_t1 = (double) Nodes[i].p_t1 / N;

        sum_P_1 = sum_P_1 + Nodes[i].p_t1;
        
		Nodes[i].e = 1;
        Nodes[i].e = (double) Nodes[i].e / N;
        sum_E_1 = sum_E_1 + Nodes[i].e;
    }

    // Утверждение суммы вероятностей = 1
    
     // Вывести сумму P (она должна быть = 1)
    printf("Sum of P = %f\n",sum_P_1);
    
    // Выход, если сумма P != 1
    assert(sum_P_1 = 1);
    
    printf("\n");
    
    // Выводим сумму E (должно быть = 1)
    printf("Sum of E = %f\n",sum_E_1);
    
    // Выход, если сумма Pt0 != 1
    assert(sum_E_1 = 1);

}

/***** Повторная инициализация значений P (t) и P (t + 1) *****/

void* P_reinit(void* arg)
{

	Thread *thread_data = (Thread *)arg;
	int i;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
			Nodes[i].p_t0 = Nodes[i].p_t1;	
			Nodes[i].p_t1 = 0;
	}
	return 0;
}

/***** Основной параллельный алгоритм *****/

void* Pagerank_Parallel(void* arg)
{

	Thread *thread_data = (Thread *) arg;
	int i, j, index;

	// Каждый поток вычислит локальную сумму и добавит ее
        // к глобальной
	double temp_sum = 0;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
		if (Nodes[i].con_size == 0)
		{
			 temp_sum = temp_sum + (double) Nodes[i].p_t0 / N;
		}

		if (Nodes[i].from_size != 0)
        {
            // Вычислить общую вероятность, внесенную соседями узла
            for (j = 0; j < Nodes[i].from_size; j++)
            {
				index = Nodes[i].From_id[j];	
				Nodes[i].p_t1 = Nodes[i].p_t1 + (double) Nodes[index].p_t0 / Nodes[index].con_size;
			}
        }		
	}
	
	// Это атомная операция
	pthread_mutex_lock(&locksum);
	sum = sum + temp_sum; 
	pthread_mutex_unlock(&locksum);
	return 0;
}

/***** Вычислить локальный максимум (максимум данных потока) *****/
void* Local_Max(void* arg)
{

	Thread *thread_data = (Thread *) arg;
	int i, j;
	
	// Каждый поток найдет локальный максимум и затем проверит
        // если это глобальный
	double temp_max = -1;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
		Nodes[i].p_t1 = d * (Nodes[i].p_t1 + sum) + (1 - d) * Nodes[i].e;
 
        if (fabs(Nodes[i].p_t1 - Nodes[i].p_t0) > temp_max)
        {
            temp_max  = fabs(Nodes[i].p_t1 - Nodes[i].p_t0);
        }		
	}

	// Проверка, есть ли у нас новый глобальный максимум 
        // То это атомарная операция
	pthread_mutex_lock(&lockmax);
	
	if (max_error  < temp_max)
	{			
		max_error = temp_max;		
	}	
	pthread_mutex_unlock(&lockmax);	
	return 0;
}

/***** Алгоритм Pagerank *****/
void Pagerank()
{

 	/***** Запуск алгоритма *****/
	
    int i, j, index;
	
	// Продолжайте, если у нас еще нет конвергенции
    while (max_error > threshold)
    {
    	max_error = -1;
		sum = 0;

		// Повторная инициализация массива P
        for (i = 0; i < num_threads; i++)
        {
			pthread_create(&Threads[i], NULL, &P_reinit,(void*) &Threads_data[i]);
		}

		// Подождите, пока все потоки "поймают" эту точку
		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
        }


        // Найти P для каждой веб-страницы
        for (i = 0; i < num_threads; i++)
        {
            pthread_create(&Threads[i], NULL, &Pagerank_Parallel, (void*) &Threads_data[i]);   
        }

		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
		}


		// Find local and global max
		for (i = 0; i < num_threads; i++)
        {
            pthread_create(&Threads[i], NULL, &Local_Max, (void*) &Threads_data[i]);   
        }

		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
		}
        
        printf("Max Error in iteration %d = %f\n", iterations+1, max_error);
        iterations++;
    }

}


/***** основная функция *****/   

int main(int argc, char** argv)
{

    struct timeval start, end;
    
    int i,j,k;
	double totaltime;
	
	// Проверка входных аргументов
	if (argc < 5)
	{
		printf("Error in arguments! Three arguments required: graph filename, N, threshold and d\n");
		return 0;
	}

	// получить аргументы
	char filename[256];
	strcpy(filename, argv[1]);
	N = atoi(argv[2]);
	threshold = atof(argv[3]);
	d = atof(argv[4]); 
	num_threads = atoi(argv[5]);

	// Проверьте входные аргументы
	if ((num_threads < 1) || (num_threads > maxThreads)) 
	{
		printf("Threads number must be >= 1 and  <= %d!\n", maxThreads);
		exit(1);
	}

	Threads_Allocation();
	Nodes_Allocation();
	
	// ИЛИ читать вероятности из файлов
    Read_from_txt_file(filename);
	//Read_P_from_txt_file();
	//Read_E_from_txt_file();

    Random_P_E();

    printf("\n");

    printf("Parallel version of Pagerank\n");

    gettimeofday(&start, NULL);
    Pagerank();
	gettimeofday(&end, NULL);  

    for (i = 0; i < N; i++)
    {
        printf("P_t1[%d] = %f\n",i, Nodes[i].p_t1);
    }
    
    printf("\n");
    
	// Всего итераций
    printf("Total iterations: %d\n", iterations);


    totaltime = (((end.tv_usec - start.tv_usec) / 1.0e6+ end.tv_sec - start.tv_sec) * 1000) / 1000;

	printf("\nTotaltime = %f seconds\n", totaltime);
    
    printf("End of program!\n");
    
    return (EXIT_SUCCESS);
}
