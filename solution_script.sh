#DO SPRAWDZENIA: JAK ZACHOWUJE SIĘ MAP-REDUCE W PRZYPADKU USTAWIENIA COUNTERA NA 0 W 44 LINII (MAPPER)?
#!/bin/bash
# założenia:
#   (1) w katalogu bieżącym lokalnego systemu plików węzła master znajdują się 
#       oprócz tego pliku (skryptu) także wszystkie inne pliki wchodzące w skład Twojego projektu (zawartość Twojego pliku projekt1.zip)
#   (2) w katalogu input znajdującym się w katalogu domowym użytkownika w systemie plików HDFS w katalogach podrzędnych 
#       datasource1 oraz datasource4 znajdują się rozpakowane pliki źródłowego zestawu danych - zestaw danych (1) i (4)


echo " "
echo ">>>> usuwanie pozostałości po wcześniejszych uruchomieniach"
# usuwamy katalog output dla mapreduce (3)
if $(hadoop fs -test -d ./output_mr3) ; then hadoop fs -rm -f -r ./output_mr3; fi
# usuwamy katalog output dla ostatecznego wyniku projektu (6)
if $(hadoop fs -test -d ./output6) ; then hadoop fs -rm -f -r ./output6; fi
# usuwamy katalog plikami projektu (skryptami, plikami jar i wszystkim co musi być dostępne w HDFS do uruchomienia projektu)
if $(hadoop fs -test -d ./project_files) ; then hadoop fs -rm -f -r ./project_files; fi
# usuwamy lokalny katalog output zawierający ostateczny wynik projektu (6)
if $(test -d ./output6) ; then rm -rf ./output6; fi

echo " "
echo ">>>> kopiowanie skryptów, plików jar i wszystkiego co musi być dostępne w HDFS do uruchomienia projektu"
hadoop fs -mkdir -p project_files
# TODO: proszę dostosować poniższe polecenia tak, aby wszystkie skrypty, pliki jar i inne, 
# które muszą do uruchomienia projektu być dostępne z poziomu HDFS, znalazły się w katalogu ./project_files
hadoop fs -copyFromLocal *.jar project_files

echo " "
echo ">>>> uruchamianie zadania MapReduce - przetwarzanie (2)"
# TODO: proszę dostosować poniższe polecenie tak, aby uruchamiało ono zadanie MapReduce (2)
#przykład dla MapReduce Classic
hadoop jar actorscounter.jar ActorsCounter input/datasource1 output_mr3
hadoop fs -getmerge output_mr3 counted_actors.tsv
hadoop fs -mkdir -p output_mr3/result
hadoop fs -copyFromLocal counted_actors.tsv output_mr3/result

echo " "
echo ">>>> uruchamianie skryptu Hive - przetwarzanie (5)"
# TODO: proszę dostosować poniższe polecenie aby uruchamiało ono skrypt Hive lub Pig (5)
#przykład dla Hive
hive -f analyse_films.hql 

echo " "
echo ">>>> pobieranie ostatecznego wyniku (6) z HDFS do lokalnego systemu plików"
mkdir -p ./output6
hadoop fs -copyToLocal output6/* ./output6

echo " "
echo " "
echo " "
echo " "
echo ">>>> prezentowanie uzyskanego wyniku (6)"
cat ./output6/*