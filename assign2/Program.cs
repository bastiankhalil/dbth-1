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
            string targetTable = "assignment_2.comments_simple";
            string targetFile = "files/large-file-RC_2011-07";

            Console.WriteLine("App Start...");

            Console.WriteLine("Connect to database...");
            MySqlConnection connection = new MySqlConnection
            {
                ConnectionString = "server=localhost;user id=root;password=passpass1700;persistsecurityinfo=True;port=3306;database=assignment_2"
            };
            connection.Open();

            DateTime startTime = DateTime.Now;
            Signal(1);

            Console.WriteLine(startTime.ToString());

            // empty table
            /*
            MySqlCommand emptyTableCommand = new MySqlCommand("TRUNCATE " + targetTable, connection);
            using (MySqlDataReader reader = emptyTableCommand.ExecuteReader())
            {
                Console.WriteLine("Empty table...");
            }
            */

            
            long[] users = new long[10] { 8007026895, 8207026895, 8307026895, 8507026895, 8810702689, 9107026895, 9207026895, 9307026895, 9407026895, 9507026895 };

            const Int32 BufferSize = 128;
            int lineNumber = 0;
            using (var fileStream = File.OpenRead(targetFile))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
            {
                String line;
                while ((line = streamReader.ReadLine()) != null)
                {
                    string[] queryStrArr = new string[9];
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
                                case "score":
                                    queryStrArr[7] = wordData[1].Replace("\"", "");
                                    break;
                                case "created_utc":
                                    queryStrArr[8] = wordData[1].Replace("\"", "");
                                    break;
                                default:
                                    break;
                            }
                        }
                    }

                    lineNumber++;

                    /*
                    using (MySqlCommand command = new MySqlCommand("INSERT INTO "+ targetTable  + " (id, parent_id, link_id, name, author, body, subreddit_id, score, created_utc, user_id) " +
                "VALUES (@id, @parent_id, @link_id, @name, @author, @body, @subreddit_id, @score, @created_utc, @user_id );", connection))
                    {
                        Random random = new Random();
                        int userRandIndex  = random.Next(0, users.Length);

                        command.Parameters.AddWithValue("@id", queryStrArr[0]);
                        command.Parameters.AddWithValue("@parent_id", queryStrArr[1]);
                        command.Parameters.AddWithValue("@link_id", queryStrArr[2]);
                        command.Parameters.AddWithValue("@name", queryStrArr[3]);
                        command.Parameters.AddWithValue("@author", queryStrArr[4]);
                        command.Parameters.AddWithValue("@body", queryStrArr[5]);
                        command.Parameters.AddWithValue("@subreddit_id", queryStrArr[6]);
                        command.Parameters.AddWithValue("@score", queryStrArr[7]);
                        command.Parameters.AddWithValue("@created_utc", queryStrArr[8]);
                        command.Parameters.AddWithValue("@user_id", users[userRandIndex]);
                        command.ExecuteNonQuery();
                    }
                    */

                    // insert reddits
                    try
                    {
                        using (MySqlCommand command = new MySqlCommand("INSERT INTO assignment_2.subreddits (id, subreddit) VALUES (@id, @subreddit );", connection))
                        {
                            command.Parameters.AddWithValue("@id", queryStrArr[6]);
                            command.Parameters.AddWithValue("@subreddit", queryStrArr[7]);
                            command.ExecuteNonQuery();
                        }
                    }
                    catch (Exception)
                    {}

                }
            }

            DateTime endTime = DateTime.Now;

            TimeSpan duration = endTime.Subtract(startTime);

            Console.WriteLine("----------------------------------------------------------------");
            Console.WriteLine("Imported File:" + targetFile );
            Console.WriteLine("Target Table:" + targetTable);
            Console.WriteLine("Duration " + duration.ToString() );

            connection.Close();
            Console.WriteLine("Connection Closed");
            Signal(20);
            Console.ReadKey();
        }


        // beep
        public static void Signal(int count)
        {

            for (int i = 1; i <= count; i++)
            {
                Console.Beep();
            }
        }
    
    }
       
}
