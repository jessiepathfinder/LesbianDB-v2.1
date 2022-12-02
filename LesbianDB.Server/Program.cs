using System;
using System.Collections.Generic;
using CommandLine;

namespace LesbianDB.Server
{
	public static class Program
	{
		private sealed class Options
		{
			[Option("listen", Required = true, HelpText = "The websocket prefix to listen to (e.g https://lesbiandb-eu.scalegrid.com/c160d449395b5fbe70fcd18cef59264b/)")]
			public string Listen { get; set; }
			[Option("engine", Required = true, HelpText = "The storage engine to use (yuri/leveldb/yuri2)")]
			public string Engine { get; set; }
			[Option("persist-dir", Required = false, HelpText = "The directory used to store the leveldb/zstore on-disk dictionary (required for leveldb, optional for yuri2, have no effect for yuri)")]
			public string ZPersistDir { get; set; }
			[Option("binlog", Required = false, HelpText = "The path of the binlog used for persistance/enhanced durability.")]
			public string Binlog{ get; set; }
			[Option("cache-size", Required = false, HelpText = "The maximum amount of cache that can be allocated by any storage engines")]
			public long CacheSize { get; set; }
			

		}
		private static void Main(string[] args)
		{
			Console.WriteLine("LesbianDB v2.1 server\nMade by Jessie Lesbian (Discord: jessielesbian#8060)\n");
			Parser.Default.ParseArguments<Options>(args).WithParsed(Main2);
			
		}
		private static void Main2(Options options){
			string engine = options.Engine;
			switch(engine){
				case "yuri":
					
			}
		}
	}
}
