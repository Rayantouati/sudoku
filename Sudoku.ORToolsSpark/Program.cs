using System;
using System.Diagnostics;
using System.IO;
using Microsoft.Spark.Sql;
using Sudoku.Shared;


namespace Sudoku.ORToolsSpark
{
    internal class Program
    {
        static void Main(string[] args)
        {

            //Console.WriteLine("Hello World!");
            var filePath = Path.Combine(Environment.CurrentDirectory, "sudoku.csv");
            var nbWorkers = 4;
            var nbCoresPerWorker = 1;
            var nbLignesMAx = 100;


            var chronometre = Stopwatch.StartNew();
            var sparkSession = SparkSession.Builder()
                .AppName("OrTools")
                .Config("spark.executor.cores", nbCoresPerWorker)
                .Config("spark.executor.instances", nbWorkers)
                .GetOrCreate();


            DataFrame dataFrame = sparkSession
                .Read()
                .Option("header", true)
                .Option("inferSchema", true)
                .Csv(filePath);


            DataFrame MilleSudoku = dataFrame.Limit(nbLignesMAx);


            sparkSession.Udf().Register<string, string>(
                    "SudokuUDF",
                    SolveSudoku);

            MilleSudoku.CreateOrReplaceTempView("Resolved");
            DataFrame sqlDF = sparkSession.Sql("select quizzes , SudokuUDF(quizzes) as Resolution from Resolved");
            sqlDF.Show();



            var tempsEcoule = chronometre.Elapsed;
            Console.WriteLine($"temps d'excution: {tempsEcoule.ToString()}");

        }

        public static string SolveSudoku(string strSudoku)

        {

            GridSudoku sudoku = GridSudoku.ReadSudoku(strSudoku);
            var ORTools = new ORToolsSolver();
            var sudokuResolu = ORTools.Solve(sudoku);

            return sudokuResolu.ToString();



        }

    }
}
