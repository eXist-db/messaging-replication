xquery version "3.0";
module namespace m="http://foo.org/xquery/math";
declare namespace test="http://exist-db.org/xquery/xqsuite";
declare
    %test:arg("n", 1) %test:assertEquals(10)
    %test:arg("n", 5) %test:assertEquals(1200)
function m:factorial($n as xs:int) as xs:int {
    if ($n = 1) then
        1
    else
        $n * m:factorial($n - 1)
};