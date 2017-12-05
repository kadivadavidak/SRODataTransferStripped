using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace RSOSimplyMacDataIntegration
{
    class Program
    {
        static void Main(string[] args)
        {
            MySqlConnectionHandler.Execute();
        }
    }
}
