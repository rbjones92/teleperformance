# Write a SQL query to find referees and the number of matches they worked in each venue

SELECT rm.referee_name, sv.venue_name, count(venue_name) as matches_worked
FROM match_mast as mm
INNER JOIN
soccer_venue as sv
ON mm.venue_id = sv.venue_id
INNER JOIN
referee_mast as rm
ON mm.referee_id = rm.referee_id
GROUP BY referee_name
ORDER BY mm.referee_id, venue_name

/*
Damir Skomina	Stade Pierre Mauroy	4
Martin Atkinson	Parc des Princes	3
Felix Brych	Stade VElodrome	3
Cuneyt Cakir	Stade de France	3
Mark Clattenburg	Stade de France	4
Jonas Eriksson	Stade de Lyon	3
Viktor Kassai	Stade de Bordeaux	3
Bjorn Kuipers	Stade de France	3
Szymon Marciniak	Stade Pierre Mauroy	3
Milorad Mazic	Stadium de Toulouse	3
Nicola Rizzoli	Stade VElodrome	4
Carlos Velasco Carballo	Stade Bollaert-Delelis	3
William Collum	Stade Bollaert-Delelis	2
Ovidiu Hategan	Stade Pierre Mauroy	2
Sergei Karasev	Stade VElodrome	2
Pavel Kralovec	Stade de Lyon	2
Svein Oddvar Moen	Stade VElodrome	2
Clement Turpin	Parc des Princes	2
*/