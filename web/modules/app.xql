xquery version "3.0";

module namespace app="http://exist-db.org/extension/jms/templates";

import module namespace templates="http://exist-db.org/xquery/templates" ;
import module namespace config="http://exist-db.org/extension/jms/config" at "config.xqm";
import module namespace jms="http://exist-db.org/xquery/messaging" at "java:org.exist.messaging.xquery.MessagingModule";
                        
(:~
 : This is a sample templating function. It will be called by the templating module if
 : it encounters an HTML element with an attribute: data-template="app:test" or class="app:test" (deprecated). 
 : The function has to take 2 default parameters. Additional parameters are automatically mapped to
 : any matching request or function parameter.
 : 
 : @param $node the HTML node with the attribute which triggered this call
 : @param $model a map containing arbitrary data - used to pass information between template calls
 :)
declare function app:test($node as node(), $model as map(*)) {
    <p>Dummy template output generated by function app:test at {current-dateTime()}. The templating
        function was triggered by the class attribute <code>class="app:test"</code>.</p>
};

declare function app:show($node as node(), $model as map(*)) {

    <table id="manageTable" class="table table-striped table-hoover table-bordered table-condensed tablesorter table-scrollable">
    <thead>
        <tr>
            <th>Id</th><th>State</th><th>Action</th><th>URL</th><th>Destination</th><th>Client-id</th><th>P</th><th>F</th><th>E</th><th> </th>
        </tr>
    </thead>
    <tbody>
    {                    
        for $id in jms:list()
        let $report := jms:report($id)
        let $nrErrors := count($report/errorMessages/error)
        return
            <tr>
            <td>{data($report/@id)}</td>
            <td>{data($report/state)}</td>
            <td><i class="icon-play"/><i class="icon-pause"/><i class="icon-ban-circle"/></td>
            <td>{data($report/java.naming.provider.url)}</td>
            <td>{data($report/destination)}</td>
            <td>{data($report/connection.client-id)}</td>
            <td>{data($report/statistics/nrProcessedMessages)}</td>
            <td>{data($report/statistics/nrUnprocessedMessages)}</td>
            
            <td style="{ if($nrErrors eq 0) then '' else  'background-color:#f2dede;'}">{ 

                if($nrErrors eq 0) 
                then
                   $nrErrors 
                else
                    <a id="error" href="#" data-html="true" data-toggle="tooltip" title="{data($report/errorMessages/error)}">{$nrErrors}</a>
            }</td>
            <td><i class="icon-info-sign"/></td>
            </tr>
    } 
    </tbody>
    </table>

};