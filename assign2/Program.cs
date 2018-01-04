using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Text;

namespace assign2
{
    class Program
    {
          static void Main(string[] args)
        {

            // define vars
            string targetTable = "assignment_2.simple";
            string targetFile = "files/test-file";

            Console.WriteLine("App Start...");

            Console.WriteLine("Connect to database...");
            MySqlConnection connection = new MySqlConnection
            {
                ConnectionString = "server=localhost;user id=root;password=passpass1700;persistsecurityinfo=True;port=3306;database=assignment_2"
            };
            connection.Open();

            DateTime startTime = DateTime.Now;

            Console.WriteLine(startTime.ToString());

            // empty table
            MySqlCommand emptyTableCommand = new MySqlCommand("TRUNCATE " + targetTable, connection);
            using (MySqlDataReader reader = emptyTableCommand.ExecuteReader())
            {
                Console.WriteLine("Empty table...");
            }


            const Int32 BufferSize = 128;
            int lineNumber = 0;
            using (var fileStream = File.OpenRead(targetFile))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
            {
                String line;
                while ((line = streamReader.ReadLine()) != null)
                {
                    string[] queryStrArr = new string[10];
                    string stipedLine = line.Replace("}", "").Replace("{", "");
                    string[] lineResult = stipedLine.Split(',');
                    foreach (string word in lineResult)
                    {
                        // Console.WriteLine(word);
                        string[] wordData = word.Split(":");
                        string wordKey = wordData[0].Replace("\"", "");
                        if (wordData.Length > 1)
                        {                        
                            switch (wordKey)
                            {
                                case "id":
                                    Console.WriteLine(lineNumber + " " +  word);
                                    string id = wordData[1].Replace("\"", "");
                                    queryStrArr[0] = id;
                                    break;
                                case "parent_id":
                                    queryStrArr[1] = wordData[1].Replace("\"", "");
                                    break;
                                case "link_id":
                                    queryStrArr[2] = wordData[1].Replace("\"", "");
                                    break;
                                case "name":
                                    queryStrArr[3] = wordData[1].Replace("\"", "");
                                    break;
                                case "author":
                                    queryStrArr[4] = wordData[1].Replace("\"", "");
                                    break;
                                case "body":
                                    queryStrArr[5] = wordData[1].Replace("\"", "");
                                    break;
                                case "subreddit_id":
                                    queryStrArr[6] = wordData[1].Replace("\"", "");
                                    break;
                                case "subreddit":
                                    queryStrArr[7] = wordData[1].Replace("\"", "");
                                    break;
                                case "score":
                                    queryStrArr[8] = wordData[1].Replace("\"", "");
                                    break;
                                case "created_utc":
                                    queryStrArr[9] = wordData[1].Replace("\"", "");
                                    break;
                                default:
                                    break;
                            }
                        }
                    }

                    lineNumber++;


                    using (MySqlCommand command = new MySqlCommand("INSERT INTO "+ targetTable  + " (id, parent_id, link_id, name, author, body, subreddit_id, subreddit, score, created_utc) " +
                "VALUES (@id, @parent_id, @link_id, @name, @author, @body, @subreddit_id, @subreddit, @score, @created_utc );", connection))
                    {

                        command.Parameters.AddWithValue("@id", queryStrArr[0]);
                        command.Parameters.AddWithValue("@parent_id", queryStrArr[1]);
                        command.Parameters.AddWithValue("@link_id", queryStrArr[2]);
                        command.Parameters.AddWithValue("@name", queryStrArr[3]);
                        command.Parameters.AddWithValue("@author", queryStrArr[4]);
                        command.Parameters.AddWithValue("@body", queryStrArr[5]);
                        command.Parameters.AddWithValue("@subreddit_id", queryStrArr[6]);
                        command.Parameters.AddWithValue("@subreddit", queryStrArr[7]);
                        command.Parameters.AddWithValue("@score", queryStrArr[8]);
                        command.Parameters.AddWithValue("@created_utc", queryStrArr[9]);
                        command.ExecuteNonQuery();
                    }

                }
            }

            DateTime endTime = DateTime.Now;

            TimeSpan duration = endTime.Subtract(startTime);

            Console.WriteLine("----------------------------------------------------------------");
            Console.WriteLine("Imported File:" + targetFile );
            Console.WriteLine("Target Table:" + targetTable);
            Console.WriteLine("Duration" + duration.ToString() );
            Console.WriteLine("Minutes:" + duration.TotalMinutes.ToString() + " Seconds:" + duration.TotalSeconds.ToString() + " Milliseconds:" + duration.TotalMilliseconds.ToString());

            connection.Close();
            Console.WriteLine("Connection Closed");
            Console.ReadKey();
        }
    }
}
