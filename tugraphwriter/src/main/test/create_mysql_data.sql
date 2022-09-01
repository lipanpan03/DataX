CREATE TABLE `movies` (
                          `mid`  varchar(200) NOT NULL,
                          `name` varchar(100) NOT NULL,
                          `year` int(11) NOT NULL,
                          `rate` float(5,2) unsigned NOT NULL,
  PRIMARY KEY (`mid`)
);

insert into
    test.movies (mid, name, year, rate)
values
    ('tt0188766', 'King of Comedy', 1999, 7.3),
    ('tt0286112', 'Shaolin Soccer', 2001, 7.3),
    ('tt4701660', 'The Mermaid',   2016,  6.3);