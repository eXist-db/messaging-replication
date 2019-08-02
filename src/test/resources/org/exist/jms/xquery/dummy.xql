xquery version "3.0";

module namespace m="http://foo.org/xquery/sometest";

declare namespace test="http://exist-db.org/xquery/xqsuite";

declare
    %test:assertEquals(10)
function m:testSimple() as xs:int {
    10
};