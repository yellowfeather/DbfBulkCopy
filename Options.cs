using CommandLine;

public class Options
{
  [Option(Required=true, HelpText="The database server")]
  public string Server { get; set; }

  [Option(Required=true, HelpText="The name of the database")]
  public string Database { get; set; }

  [Option(Default=true, Required = false, HelpText = "Use user's credentials to connect to database Server")]
  public bool UseSSPI { get; set; }

  [Option(Required=false, HelpText="The UserID used to connect to the database server")]
  public string UserId { get; set; }

  [Option(Required=false, HelpText="The password used to connect to the database server")]
  public string Password { get; set; }

  [Option(Required=true, HelpText="Path to the DBF file to import")]
  public string Dbf { get; set; }

  [Option(Required=true, HelpText="The name of the database table to import into")]
  public string Table { get; set; }

  [Option(Default=30, HelpText="The connection timeout used in the bulk copy operation")]
  public int BulkCopyTimeout { get; set; }

  [Option(Default=false, HelpText="Whether to truncate the table before copying")]
  public bool Truncate { get; set; }

  [Option(Default=false, HelpText="Whether to skip deleted records")]
  public bool SkipDeletedRecords { get; set; }
  
  [Option(Default = false, HelpText = "Create destination table. Drop it first if it exists ")]
  public bool CreateTable { get; set; }
}