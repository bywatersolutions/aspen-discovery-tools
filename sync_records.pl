#!/usr/bin/perl

use Modern::Perl;

use Config::Tiny;
use DBI;
use Data::Dumper;
use Getopt::Long::Descriptive;
use Array::Utils qw(array_minus);

my ( $opt, $usage ) = describe_options(
    '%c %o',
    [ 'ini|i=s', "Path to the INI file", { required => 1 } ],
    [ 'fix|f', "Insert zebra queue rows in Koha to correct the issues found" ],
    [],
    [ 'verbose|v', "print extra stuff" ],
    [ 'help|h',    "print usage message and exit", { shortcircuit => 1 } ],
);
print( $usage->text ), exit if $opt->help;

my $config = Config::Tiny->read( $opt->ini );
if ( !$config ) {
    die "Failed to read INI file: " . Config::Tiny->errstr . "\n";
}

# Create the Aspen connection
my $database_user         = $config->{Database}->{database_user};
my $database_password     = $config->{Database}->{database_password};
my $database_dsn          = $config->{Database}->{database_dsn};
my $database_aspen_dbname = $config->{Database}->{database_aspen_dbname};

# Remove outer quotes
$database_dsn =~ s/^(['"])(.*)\1$/$2/;

# Ensure all necessary keys are present
if (   !defined $database_user
    || !defined $database_password
    || !defined $database_dsn )
{
    die "Missing one or more required database connection keys"
      . " (database_user, database_pass, database_dsn) in the INI file\n";
}

# Connect to the database
my $aspen_dbh = DBI->connect(
    "DBI:$database_dsn",
    $database_user,
    $database_password,
    {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 1,
    }
);

if ($aspen_dbh) {
    say "Successfully connected to the Aspen database." if $opt->verbose;
}
else {
    die "Failed to connect to the Aspen database: " . DBI->errstr . "\n";
}

# Create the Koha connection
my $sth =
  $aspen_dbh->prepare(q{SELECT * FROM account_profiles WHERE driver = 'Koha'});

my $rows = $sth->execute();
if ( $rows > 1 ) {
    say "Found multiple Koha configs, I don't know what to do!";
    exit 1;
}

my $koha_config = $sth->fetchrow_hashref;
my $koha_dsn =
    "DBI:mysql:"
  . "database=$koha_config->{databaseName}"
  . ";host=$koha_config->{databaseHost}"
  . ";port=$koha_config->{databasePort}";
my $koha_dbh = DBI->connect(
    $koha_dsn,
    $koha_config->{databaseUser},
    $koha_config->{databasePassword},
    {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 1,
    }
);

if ($koha_dbh) {
    say "Successfully connected to the Koha database." if $opt->verbose;
}
else {
    die "Failed to connect to the Koha database: " . DBI->errstr . "\n";
}

# Get the biblionumbers from Koha
my $koha_biblionumbers = $koha_dbh->selectcol_arrayref(
    q{
    SELECT biblionumber FROM biblio
}
);
say "KOHA RECORDS: " . scalar @$koha_biblionumbers if $opt->verbose;

# Get the biblionumbers from Aspen
my $aspen_biblionumbers = $aspen_dbh->selectcol_arrayref(
    q{
    SELECT ilsId FROM ils_records WHERE source = 'ils'
}
);
say "ASPEN RECORDS: " . scalar @$aspen_biblionumbers if $opt->verbose;

my @records_in_koha_not_in_aspen =
  array_minus( @$koha_biblionumbers, @$aspen_biblionumbers );
say "RECORDS FOUND IN KOHA NOT FOUND IN ASPEN: "
  . scalar @records_in_koha_not_in_aspen;

my @records_in_aspen_not_in_koha =
  array_minus( @$aspen_biblionumbers, @$koha_biblionumbers );
say "RECORDS FOUND IN ASPEN NOT FOUND IN KOHA: "
  . scalar @records_in_aspen_not_in_koha;

if ( $opt->fix && scalar @records_in_koha_not_in_aspen ) {
    say "Fixing records found in Koha but not in Aspen";
    my $count = scalar @records_in_koha_not_in_aspen;
    my $i     = 1;
    $sth = $koha_dbh->prepare(
        q{
        INSERT INTO zebraqueue ( id, biblio_auth_number, operation, server, done, time )
	VALUES ( NULL, ?, 'specialUpdate', 'biblioserver', 1, NOW() )
    }
    );
    foreach my $id (@records_in_koha_not_in_aspen) {
        print "Inserting zebra queue update for record $id:  $i of $count\r";
        $sth->execute($id);
        $i++;
    }
}

if ( $opt->fix && scalar @records_in_aspen_not_in_koha ) {
    say "Fixing records found in Aspen but not in Koha";
    my $count = scalar @records_in_aspen_not_in_koha;
    my $i     = 1;
    #my $sql = q{ INSERT INTO zebraqueue ( id, biblio_auth_number, operation, server, done, time ) VALUES ( NULL, ?, 'recordDelete', 'biblioserver', 1, NOW() ) };
    my $sql = q{ INSERT INTO deletedbiblio ( biblionumber, title, datecreated  ) VALUES ( ?, "Fixing bad record in Aspen", NOW()  ) };
    $sth = $koha_dbh->prepare($sql);
    foreach my $id (@records_in_aspen_not_in_koha) {
        print "Inserting zebra queue update for record $id:  $i of $count\r";
        $sth->execute($id);
        $i++;
    }
}
