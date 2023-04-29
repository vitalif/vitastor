#!/usr/bin/perl

use strict;

for my $line (<>)
{
    if ($line =~ /\.\/(test_[^\.]+)/s)
    {
        chomp $line;
        my $test_name = $1;
        my $timeout = 3;
        if ($test_name eq 'test_etcd_fail' || $test_name eq 'test_heal' || $test_name eq 'test_interrupted_rebalance')
        {
            $timeout = 10;
        }
        while ($line =~ /([^\s=]+)=(\S+)/gs)
        {
            if ($1 eq 'SCHEME' && $2 eq 'ec')
            {
                $test_name .= '_ec';
            }
            elsif ($1 eq 'SCHEME' && $2 eq 'xor')
            {
                $test_name .= '_xor';
            }
            elsif ($1 eq 'IMMEDIATE_COMMIT')
            {
                $test_name .= '_imm';
            }
            else
            {
                $test_name .= '_'.lc($1).'_'.$2;
            }
        }
        $line =~ s!\./test_!/root/vitastor/tests/test_!;
        # Gitea CI doesn't support artifacts yet, lol
        #- name: Upload results
        #  uses: actions/upload-artifact\@v3
        #  if: always()
        #  with:
        #    name: ${test_name}_result
        #    path: |
        #      /root/vitastor/testdata
        #      !/root/vitastor/testdata/*.bin
        #    retention-days: 5
        print <<"EOF"
  $test_name:
    runs-on: ubuntu-latest
    needs: build
    container: \${{env.TEST_IMAGE}}:\${{github.sha}}
    steps:
    - name: Run test
      id: test
      timeout-minutes: $timeout
      run: $line
    - name: Print logs
      if: always() && steps.test.outcome == 'failure'
      run: |
        for i in /root/vitastor/testdata/*.log /root/vitastor/testdata/*.txt; do
          echo "-------- \$i --------"
          cat \$i
          echo ""
        done

EOF
;
    }
}
