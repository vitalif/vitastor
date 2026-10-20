#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

# pipe in - pipe out
dd if=/dev/urandom of=./testdata/testfile bs=1M count=128
$VITASTOR_CLI dd oimg=testimg iodepth=4 bs=1M count=128 < ./testdata/testfile
$VITASTOR_CLI dd iimg=testimg iodepth=4 bs=1M count=128 > ./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# snapshot
dd if=/dev/urandom of=./testdata/over bs=1M count=4
dd if=./testdata/over of=./testdata/testfile bs=1M seek=17 conv=notrunc
$VITASTOR_CLI snap-create testimg@snap1
$VITASTOR_CLI dd iodepth=4 if=./testdata/over oimg=testimg bs=1M seek=17
$VITASTOR_CLI dd iodepth=4 iimg=testimg of=./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# copy image using dd
$VITASTOR_CLI dd iimg=testimg iodepth=4 bs=1M count=128 | $VITASTOR_CLI dd oimg=testimg2 iodepth=4 bs=1M count=128
$VITASTOR_CLI dd iimg=testimg2 iodepth=4 bs=1M count=128 > ./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# size= sets the output size, the input size is still detected
$VITASTOR_CLI dd iimg=testimg oimg=testimg3 iodepth=4 size=192M
$VITASTOR_CLI dd if=./testdata/testfile oimg=testimg4 iodepth=4 size=192M
for img in testimg3 testimg4; do
    size=$($VITASTOR_CLI ls --json | jq -r '.[] | select(.name == "'$img'") | .size')
    [[ "$size" = $((192*1024*1024)) ]] || format_error "dd size=192M created $img with size $size"
    $VITASTOR_CLI dd iimg=$img iodepth=4 bs=1M count=128 > ./testdata/testfile1
    diff ./testdata/testfile ./testdata/testfile1
    rm ./testdata/testfile1
done

# skip beyond the end of input fails before the output image is created
rc=0
$VITASTOR_CLI dd iimg=testimg oimg=testimg5 bs=1M skip=200 count=1 || rc=$?
[[ $rc = 22 ]] || format_error "dd skip beyond the end of input returned $rc instead of EINVAL"
if $VITASTOR_CLI ls --json | jq -e '.[] | select(.name == "testimg5")'; then
    format_error "dd skip beyond the end of input created the output image"
fi

# status=none prints nothing, also with the default --progress 1
$VITASTOR_CLI dd iimg=testimg of=./testdata/testfile1 iodepth=4 status=none 2>./testdata/dd.err
[[ ! -s ./testdata/dd.err ]] || format_error "dd status=none printed: $(cat ./testdata/dd.err)"
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# status=noxfer drops the final statistics line
$VITASTOR_CLI --progress 0 dd iimg=testimg of=./testdata/testfile1 iodepth=4 status=noxfer 2>./testdata/dd.err
[[ ! -s ./testdata/dd.err ]] || format_error "dd status=noxfer printed: $(cat ./testdata/dd.err)"
rm ./testdata/testfile1
# with --progress 1 the progress line, if it was shown at all, is cleared at the end
clear_line=$'\r\033[K'
for count in 128 4; do
    $VITASTOR_CLI dd iimg=testimg of=./testdata/testfile1 iodepth=4 bs=1M count=$count status=noxfer 2>./testdata/dd.err
    if [[ $(wc -l < ./testdata/dd.err) != 0 || ( -s ./testdata/dd.err && "$(cat ./testdata/dd.err)" != *copied*"$clear_line" ) ]]; then
        format_error "dd status=noxfer count=$count printed: $(cat -v ./testdata/dd.err)"
    fi
    head -c ${count}M ./testdata/testfile | cmp - ./testdata/testfile1
    rm ./testdata/testfile1
done

# oflag=append appends to the output file
cp ./testdata/over ./testdata/testfile1
$VITASTOR_CLI dd iimg=testimg of=./testdata/testfile1 iodepth=4 bs=1M count=8 oflag=append
(cat ./testdata/over; head -c 8M ./testdata/testfile) | cmp - ./testdata/testfile1 || format_error "dd oflag=append did not append to the output file"
rm ./testdata/testfile1
# conv=trunc contradicts oflag=append: fail without touching the output file
cp ./testdata/over ./testdata/testfile1
rc=0
$VITASTOR_CLI dd iimg=testimg of=./testdata/testfile1 iodepth=4 bs=1M count=8 oflag=append conv=trunc || rc=$?
[[ $rc = 22 ]] || format_error "dd conv=trunc oflag=append returned $rc instead of EINVAL"
cmp ./testdata/over ./testdata/testfile1 || format_error "dd conv=trunc oflag=append changed the output file"
rm ./testdata/testfile1

format_green OK
