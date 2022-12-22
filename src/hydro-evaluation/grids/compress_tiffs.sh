for FILE in data/*; do 
    mv $FILE $FILE.org
    gdal_translate -co "COMPRESS=LZW" $FILE.org $FILE
    rm $FILE.org
done