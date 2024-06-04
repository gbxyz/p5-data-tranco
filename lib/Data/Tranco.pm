package Data::Tranco;
# ABSTRACT: An interface to the Tranco domain list.
use Archive::Zip qw(:ERROR_CODES);
use Archive::Zip::MemberRead;
use Carp;
use DBD::SQLite;
use DBI;
use Data::Mirror qw(mirror_file);
use File::Basename qw(basename dirname);
use File::Spec;
use File::stat;
use POSIX qw(getlogin);
use Text::CSV_XS;
use constant TRANCO_URL => 'https://tranco-list.eu/top-1m.csv.zip';
use feature qw(state);
use open qw(:encoding(utf8));
use strict;
use utf8;
use vars qw($TTL $ZIPFILE $DBFILE $DSN $STATIC);
use warnings;

$TTL        = 86400;
$ZIPFILE    = Data::Mirror::filename(TRANCO_URL);
$DBFILE     = File::Spec->catfile(dirname($ZIPFILE), basename($ZIPFILE, '.zip').'.db');
$DSN        = 'dbi:SQLite:dbname='.$DBFILE;
$STATIC     = undef;

=pod

=head1 SYNOPSIS

    use Data::Tranco;

    # get a random domain from the list
    ($domain, $rank) = Data::Tranco->random_domain;

    # get a random domain from .org
    ($domain, $rank) = Data::Tranco->random_domain("org");

    # get the highest ranking domain
    ($domain, $rank) = Data::Tranco->top_domain;

    # get the highest ranking domain in .co.uk
    ($domain, $rank) = Data::Tranco->top_domain("co.uk");

    # get a sample of 50 domains
    @domains = Data::Tranco->sample(50);

    # get a sample of 50 domains in .org
    @domains = Data::Tranco->top_domains(50, "org");

    # get all 1,000,000 domains
    @all = Data::Tranco->all;

    # get all domains in .org
    @all = Data::Tranco->all("org");

    # get the top 50 domains in .jp
    @domains = Data::Tranco->top_domains(50, "jp");

    # get the ranking of perl.org
    $rank = Data::Tranco->rank("perl.org");

=head1 DESCRIPTION

C<Data::Tranco> provides an interface to the L<Tranco|https://tranco-list.eu>
list of popular domain names.

=head1 METHODS

=head2 RANDOM DOMAIN

    ($domain, $rank) = Data::Tranco->random_domain($suffix);

Returns a randomly-selected domain from the list, along with its ranking.
If C<$suffix> is specified, then only a domain that ends with that suffix will
be returned.

=cut

sub random_domain {
    my ($package, $suffix) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT * FROM `domains`
        WHERE `domain` LIKE ?
        ORDER BY RANDOM()
        LIMIT 0,1});

    $sth->execute($suffix ? '%.'.$suffix : '%');

    return reverse($sth->fetchrow_array);
}

=pod

=head2 TOP DOMAIN

    ($domain, $rank) = Data::Tranco->top_domain($suffix);

Returns the highest-ranking domain from the list, along with its ranking. If
C<$suffix> is specified, then the highest-ranking domain that ends with that
suffix will be returned.

=cut

sub top_domain {
    my ($package, $suffix) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT * FROM `domains`
        WHERE `domain` LIKE ?
        ORDER BY `id`
        LIMIT 0,1});

    $sth->execute($suffix ? '%.'.$suffix : '%');

    return reverse($sth->fetchrow_array);
}

=pod

=head2 SAMPLE

    @domains = Data::Tranco->sample($count, $suffix);

Returns an array containing C<$count> randomly-selected domains. If C<$suffix> is
specified, only domains ending with that suffix will be returned.

=cut

sub sample {
    my ($package, $count, $suffix) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT `domain` FROM `domains`
        WHERE `domain` LIKE ?
        ORDER BY RANDOM()
        LIMIT 0,?});

    $sth->execute($suffix ? '%.'.$suffix : '%', int($count));

    my @domains;

    while (my @row = $sth->fetchrow_array) {
        push(@domains, @row);
    }

    return @domains;
}

=pod

=head2 TOP N DOMAINS

    @domains = Net::Tranco->top_domains($count, $suffix);

Returns an array of the highest ranking C<$count> domains. If C<$suffix> is
specified, only domains ending with that suffix will be returned. The number of
entries in the array may be less than C<$count> if the TLD is small and/or
C<$count> is high.

=cut

sub top_domains {
    my ($package, $count, $suffix) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT `domain` FROM `domains`
        WHERE `domain` LIKE ?
        ORDER BY `id`
        LIMIT 0,?});

    $sth->execute($suffix ? '%.'.$suffix : '%', int($count));

    my @domains;

    while (my @row = $sth->fetchrow_array) {
        push(@domains, @row);
    }

    return @domains;
}

=pod

=head2 ALL DOMAINS

    @domains = Net::Tranco->all($suffix);

Returns an array of all domains. If C<$suffix> is specified, only domains ending
with that suffix will be returned, otherwise, you'll get all 1M domains!

=cut

sub all {
    my ($package, $suffix) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT `domain` FROM `domains`
        WHERE `domain` LIKE ?
        ORDER BY `id`});

    $sth->execute($suffix ? '%.'.$suffix : '%');

    my @domains;

    while (my @row = $sth->fetchrow_array) {
        push(@domains, @row);
    }

    return @domains;
}

=pod

=head1 DOMAIN RANK

    $rank = Net::Tranco->rank($domain);

Returns the ranking of the domain C<$domain> or C<undef> if the domain isn't
present in the list.

=cut

sub rank {
    my ($package, $domain) = @_;

    state $sth = $package->get_db->prepare(q{
        SELECT `id`
        FROM `domains`
        WHERE (`domain`=?)
    });

    $sth->execute($domain);

    return $sth->fetchrow_array;
}

=pod

=head1 DATABASE HANDLE

    $db = Data::Tranco->get_db;

Returns a L<DBI> object so you can perform your own queries. The database
contains a single table called `domains`, which has the `id` and `domain`
columns containing the ranking and domain name, respectively.

=cut

sub get_db {
    my $package = shift;

    state $db;

    if (!$db) {
        $package->update_db if ($package->needs_update);
    
        $db = DBI->connect($DSN);
    }

    return $db;
}

=pod

=head1 IMPLEMENTATION

The Tranco list is published as a zip-compressed CSV file. By default,
C<Data::Tranco> will automatically download that file, extract the CSV file,
and write it to an L<SQLite|DBD::SQLite> database if (a) the file doesn't exist
yet or (b) it's more than a day old.

If you want to control this behaviour, you can use the following:

=head2 C<$Data::Tranco::TTL>

This is how old the local file can be (in seconds) before it is updated. It is
86400 seconds by default.

=head2 C<$Data::Tranco::STATIC>

If you set this value to C<1> then C<Data::Tranco> will not update the database,
even if it doesn't exist, in which case, all the methods above will fail.

=head2 C<Data::Tranco-E<gt>update_db>

This will cause C<Data::Tranco> to update its database. If it fails it will
C<croak()>, so calls to this method should be wrapped in an `eval`.

=cut

#
# returns true if the database needs updating, that is:
#
# 0. $STATIC is not defined
# 1. the DB file doesn't exist
# 2. the zip file doesn't exist
# 3. the DB file is older than the zip file
# 4. the zip file is more than TTL seconds old
#
sub needs_update {
    my $package = shift;

    return undef if ($STATIC);

    return 1 unless (-e $DBFILE && -e $ZIPFILE);
    return 1 unless (stat($DBFILE)->mtime > stat($ZIPFILE)->mtime);
    return 1 unless (stat($ZIPFILE)->mtime > time() - $TTL);

    return undef;
}

sub update_db {
    my $package = shift;

    mirror_file(TRANCO_URL, $TTL);

    my $zip = Archive::Zip->new;

    croak('Zip read error') unless ($zip->read($ZIPFILE) == AZ_OK);

    my $db = DBI->connect($DSN, undef, undef, { AutoCommit => 0 });

    $db->do(q{
        CREATE TABLE IF NOT EXISTS `domains` (
            `id`        INTEGER PRIMARY KEY,
            `domain`    TEXT UNIQUE COLLATE NOCASE
        )
    });

    my $sth = $db->prepare(q{INSERT INTO `domains` (`id`, `domain`) VALUES (?, ?)});

    $db->do(q{DELETE FROM `domains`});

    my $fh  = Archive::Zip::MemberRead->new($zip, basename(TRANCO_URL, '.zip'));
    my $csv = Text::CSV_XS->new;
    while (my $row = $csv->getline($fh)) {
        $sth->execute(@{$row});
    }

    $db->commit;
    $db->disconnect;
}

1;
