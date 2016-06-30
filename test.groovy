@Grab('io.reactivex:rxgroovy:1.0.3')
@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.7')

import rx.Observable;
import rx.observables.GroupedObservable;
import rx.Observer;
import rx.schedulers.Schedulers;
import rx.Subscriber;
import rx.Subscription;
import rx.subscriptions.Subscriptions;
import rx.functions.Action0
import rx.functions.Func1;
import groovyx.net.http.RESTClient
import java.util.concurrent.TimeUnit

@RestController
@ConfigurationProperties
class RxTest {
  String worldName
  String meetupApiKey 
  String meetupBaseUrl
  Random rando = new Random()

  def randInt(Integer max) {
    Math.abs(rando.nextInt() % max)
  }

  def simulateRequest(Integer n, Integer delay) {
    Thread.sleep(delay)
    n
  }

  def simulateAsyncRequest(Integer n, Integer delay) {
    Observable.create { Subscriber s -> 
      simulateRequest(n,delay)
      s.onNext(n);
      s.onCompleted();
    }
  }

  Observable rateLimit(Long rate, Observable obs) {
    def timer = Observable.interval(rate, TimeUnit.MILLISECONDS)
    Observable.zip(timer,obs, { t,o -> o } )
  }

  List waitFor(Observable obs) {
    obs
    .toBlocking()
    .toIterable()
    .toList()
  }

  Observable countGroups(Observable<GroupedObservable> groups) {
    groups.flatMap { grp ->
      def key = grp.key
      def grpList = grp.count()
      Observable.zip(Observable.from(key),grpList, { k,c -> [key:k, count:c] } )
    }
  }

  @RequestMapping("/")
  def home() {
    "Hello $worldName!"
  }

  @RequestMapping("/rxcount")
  def rxcount(@RequestParam Integer upperBound) {
    waitFor( 
      countGroups( 
        Observable.from(0..upperBound)
          .map { [n: it, delay: randInt(3) ] }
          .groupBy { it.delay }
      )
    )
  }

  @RequestMapping("/range")
  def range(@RequestParam Integer upperBound) {
    def r = 0..upperBound
    r.collect {         
      [n: it, delay: randInt(1000) ]
    }
  }  

  @RequestMapping("/processrange")
  def processrange(@RequestParam Integer upperBound) {
    def r = 0..upperBound
    def processed = r.collect {         
      [n: it, delay: randInt(1000) ]
    }.collect {
      simulateRequest(it['n'], it['delay'])
    }
  }

  @RequestMapping("/rxrange")
  def rxrange(@RequestParam Integer upperBound) {
    Observable.from(0..upperBound)
      .map { [n: it, delay: randInt(1000) ] }
      .toBlocking()
      .toIterable()
  }

  @RequestMapping("/rxprocessrange")
  def rxprocessrange(@RequestParam Integer upperBound) {
    Observable.from(0..upperBound)
      .map { [n: it, delay: randInt(1000) ] }
      .flatMap { 
        simulateAsyncRequest(it.n, it.delay)
          .subscribeOn(Schedulers.io() ) 
      }
      .toBlocking()
      .toIterable()
  }

  @RequestMapping("/groups")
  def groups() {
    RESTClient client = new RESTClient(meetupBaseUrl)
    def resp = client.get( path: "/self/groups", query: [key:meetupApiKey] )
    def headers = resp.headers.collectEntries { [ (it.name): it.value ] }
    [status: resp.status, headers: headers, data: resp.data]
  }

  @RequestMapping("/events")
  def events() {
    RESTClient client = new RESTClient(meetupBaseUrl)
    def resp = client.get( path: "/self/groups", query: [key:meetupApiKey] )
    def headers = resp.headers.collectEntries { [ (it.name): it.value ] }

    def groups = resp.data.collect { [urlname: it.urlname, id: it.id, name: it.name, members: it.members] }
    def events = groups
      .collect { client.get( path: "/${it.urlname}/events", query: [key:meetupApiKey])}
      .collect { [status: it.status, data: it.data] }
    [status: resp.status, headers: headers, groups: groups, events: events]
  }

  @RequestMapping("/rxevents")
  def rxevents() {
    RESTClient client = new RESTClient(meetupBaseUrl)
    def resp = client.get( path: "/self/groups", query: [key:meetupApiKey] )
    def headers = resp.headers.collectEntries { [ (it.name): it.value ] }
    def rate = Math.round( (headers["X-RateLimit-Reset"].toInteger() * 1000) / headers["X-RateLimit-Limit"].toInteger() )
    def timer = Observable.interval(rate, TimeUnit.MILLISECONDS)

    def groups = resp.data.collect { [urlname: it.urlname, id: it.id, name: it.name, members: it.members] }

    def events = waitFor( rateLimit( rate + 50, Observable.from(groups))
      .map { println "fetching ${it.urlname} at ${System.currentTimeMillis()}"; return it }
      .map { client.get( path: "/${it.urlname}/events", query: [key:meetupApiKey]) }
      .map { [status: it.status, data: it.data] }
    )

    [status: resp.status, headers: headers, rate: rate, groups: groups, events: events ]
  }

  @RequestMapping("/members")
  def members() {
    RESTClient client = new RESTClient(meetupBaseUrl)
    def resp = client.get( path: "/self/groups", query: [key:meetupApiKey] )
    def headers = resp.headers.collectEntries { [ (it.name): it.value ] }
    def groups = resp.data.collect { [urlname: it.urlname, id: it.id, name: it.name, members: it.members] }
    def members = groups
      .collect { client.get( path: "/${it.urlname}/members", query: [key:meetupApiKey] ) }
      .collect { it.data }
      .flatten()
      .collect { [id: it.id, name: it.name ] }
    println members.size()
    def memberProfiles = members[0..20]
      .collect { client.get( path: "/members/${it.id}", query: [key:meetupApiKey, fields:"memberships"])}
      .collect { it.data }
      .flatten()
      // get all memberships of each person.  
      .collect { it?.memberships.member }
      .findAll { it }
      .flatten()
      .collect { [ id: it.group.id, name: it.group.name, urlname: it.group.urlname ] }
      // remove duplicates
      .unique(false)
    [status: resp.status, headers: headers, groups: groups, members: members, memberProfiles: memberProfiles]
  }

  @RequestMapping("/rxmembers")
  def rxmembers() {
    RESTClient client = new RESTClient(meetupBaseUrl)

    def resp = client.get( path: "/self/groups", query: [key:meetupApiKey] )
    def headers = resp.headers.collectEntries { [ (it.name): it.value ] }
    def rate = Math.round( (headers["X-RateLimit-Reset"].toInteger() * 1000) / headers["X-RateLimit-Limit"].toInteger() )

    def myGroups = resp.data.collect { [urlname: it.urlname, id: it.id, name: it.name, members: it.members] }

    def members = waitFor( rateLimit( rate + 50, Observable.from(myGroups))
      .flatMap { 
        println "fetching ${it.urlname} at ${System.currentTimeMillis()}"
        def r = client.get( path: "/${it.urlname}/members", query: [key:meetupApiKey]) 
        Observable.from( r.data )
      }
      .map { [id: it.id, name: it.name] }
    )

    def membersGroups = waitFor( 
      rateLimit( rate + 50, Observable.from(members))
        .take(30)
        .map { 
          println "fetching member ${it.id} at ${System.currentTimeMillis()}"
          def r = client.get( path: "/members/${it.id}", query: [key:meetupApiKey, fields:"memberships"])
          def h = r.headers.collectEntries { header -> [ (header.name): header.value ] }
          println "remaining: ${h['X-RateLimit-Remaining']} limit: ${h['X-RateLimit-Limit']} reset: ${h['X-RateLimit-Reset']}"
          r.data
        }
        .filter { it.memberships != null}
        .flatMap { Observable.from(it.memberships.member) }
        .map { [id: it.group.id, name: it.group.name, urlname: it.group.urlname] }
    )

    def groupCounts = waitFor( 
      countGroups(
        Observable.from(membersGroups)
          .groupBy { it.urlname }
      )
    ).sort { it.count }.reverse()

    [status: resp.status, headers: headers, rate: rate, groupCounts:groupCounts, myGroups: myGroups, membersGroups: membersGroups]    
  }
}
