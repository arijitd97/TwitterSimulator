// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp
#if INTERACTIVE
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#endif

open System
open Akka.FSharp
open Akka.Actor
open System.Threading
open Akka.Configuration
open System.IO;
open System.Text;

type UserMessageTypes =
    | InitialiseUserActor of ActorSelection * string * int
    | Follow
    | QueryHashTag
    | QueryMention
    | Tweets
    | Online
    | Offline
    | PrintParams

type ClientMessageTypes =
    | SetParameters
    | RegisterUser
    | SetOnline
    | SetOffline
    | Tweet
    | Query
    | DisplayServer

let serverIP = fsi.CommandLineArgs.[1] |> string
let noOfUsers = fsi.CommandLineArgs.[2] |> int

let myIP = "localhost"
let port = "0"

let configuration = 
    ConfigurationFactory.ParseString(
        sprintf @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = %s
                hostname = %s
            }
    }" port myIP)


let system = ActorSystem.Create("TwitterClient",configuration)


let UserActor(mailbox: Actor<'a>) =
    let mutable userId = null
    let mutable status = true
    let mutable server = null
    let mutable tweetCount = 0
    let mutable subsCount = 0
    let hashTagsList = ["lockdown";"metoo";"covid19";"blacklivesmatter";"crypto";"crowdfunding";"giveaway";"contest";
                        "blackhistorymonth";"womenshistorymonth";"cryptocurrency";"womensday";"happybirthday";
                        "authentication";"USelections";"bidenharris";"internationalwomensday";"influencermarketing";
                        "distributedsystems";"gogators";"blackfriday";"funny";"womeninstem";"iwon";"photography";
                        "mondaymotivation";"ootd";"vegan";"traveltuesday";"tbt"]

    let rec loop()= actor{
        let! msg = mailbox.Receive()
        match msg with
        | InitialiseUserActor (serverRef, Id, sCount) ->
            server <- serverRef
            userId <- Id
            subsCount <- sCount

        | Follow ->
            for i in [1..subsCount] do
                let mutable randNumber = Random().Next(1,noOfUsers)
                while string randNumber = userId do
                    randNumber <- ((randNumber+1)% noOfUsers) + 1
                server <! ("Subscribe", string randNumber, userId, DateTime.Now)

        | QueryHashTag ->
            if status then
                server <! ("SearchHashTag",userId, hashTagsList.[Random().Next(hashTagsList.Length)], DateTime.Now)

        | QueryMention ->
            if status then
                server <! ("SearchMention", userId, "", DateTime.Now)

        | Tweets ->
            if status then
                for i in [0..subsCount-1] do
                    let select = Random().Next(1,5)
                    if select = 1 then 
                        //Tweet a random string
                        // printfn "Tweeing a random string"
                        tweetCount <- tweetCount + 1
                        let s = sprintf "Hello from user %s. This is my tweet number: %d" userId tweetCount
                        server <! ("Tweet",userId,s, DateTime.Now)

                    else if select = 2 then
                        //Retweet a random string from feed
                        // printfn " Retweet"
                        tweetCount <- tweetCount + 1
                        server <! ("Retweet",userId,"", DateTime.Now)



                    else if select = 3 then
                        //Tweet using a hashtag
                        // printfn "Tweeing a random string with hashtag"
                        let mutable s = sprintf "Hello from user %s. I am using hashtags: #" userId
                        tweetCount <- tweetCount + 1
                        s <- s + hashTagsList.[Random().Next(0,hashTagsList.Length)]
                        server <! ("Tweet",userId,s, DateTime.Now)

                    else
                        //Tweet using a mention
                        // printfn "Tweeing a random string with mention"
                        let mutable s = sprintf "Hello from user %s. How are you @" userId
                        tweetCount <- tweetCount + 1
                        s <- s + (Random().Next(1,noOfUsers) |> string)
                        server <! ("Tweet",userId,s, DateTime.Now)

        | Online ->
            server <! ("ShowFeed", userId, "", DateTime.Now)
            status <- true

        | Offline ->
            status <- false

        | PrintParams ->
            printfn "User Id = %s SubsCount = %d Status = %b" userId subsCount status

           
        return! loop()
    }
    loop() 


let Client(mailbox: Actor<'a>) =
         
    let server = system.ActorSelection(sprintf "akka.tcp://TwitterServer@%s:8776/user/MasterSpace" serverIP)

    let mutable userListRef = List.Empty
    let mutable userList = List.Empty
    let mutable onlineUsers = Set.empty
    let mutable offlineUsers = Set.empty

    let rec loop()= actor{
        let! msg = mailbox.Receive()
        match msg with
        | SetParameters ->
            printfn "Client Started"
            let mutable userArr = Array.create noOfUsers ""
            let mutable userArrRef = Array.create noOfUsers null
            for i in 0..noOfUsers-1 do
                Array.set userArr i (string (i+1))
                let userActorRef = spawn system ("actor"+userArr.[i]) UserActor
                Array.set userArrRef i userActorRef
            
            for i in [0..userArr.Length-1] do
                let j = Random().Next(1,userArr.Length)
                let tmp1 = userArr.[i]
                userArr.[i] <- userArr.[j]
                userArr.[j] <- tmp1
                let tmp2 = userArrRef.[i]
                userArrRef.[i] <- userArrRef.[j]
                userArrRef.[j] <- tmp2

            userList <- Array.toList userArr
            userListRef <- Array.toList userArrRef

            for i in 0..noOfUsers-1 do
                let subsCount = max ((noOfUsers-1)/(i+1)) 1
                userListRef.[i] <! InitialiseUserActor (server,userList.[i],subsCount)



            let countOffline = (30*noOfUsers/100)|>int
            for i in 1..countOffline do
                let mutable userOff = userListRef.[Random().Next(0,noOfUsers)]
                while offlineUsers.Contains(userOff) do
                    userOff <- userListRef.[Random().Next(0,noOfUsers)]
                offlineUsers <- Set.add userOff offlineUsers
                userOff <! Offline

            for userRef in userListRef do
                if not (offlineUsers.Contains(userRef)) then
                    onlineUsers <- Set.add userRef onlineUsers

            mailbox.Self <! RegisterUser
            
        | RegisterUser ->
            for user in userList do
                server <! ("Register", user, "", DateTime.Now)

            printfn "All users have been registered. Sending requests will start soon!"

            for userRef in userListRef do
                userRef <! Follow


            Thread.Sleep(50000)
            printfn "Actions like tweeting and querying have begun.\nForce quit client using ctrl+C when server ends!"
            mailbox.Self <! Tweet
            mailbox.Self <! Query
            mailbox.Self <! SetOnline
            mailbox.Self <! SetOffline

        
        | SetOnline ->
            for user in offlineUsers do
                user <! Online
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(5.0),TimeSpan.FromMilliseconds(5000.0), mailbox.Self, SetOnline)



        | SetOffline ->
            let n = onlineUsers.Count
            let countOffline=(20*(n)/100)
            let onlineList = Set.toList onlineUsers
            for i in [1..countOffline] do
                let randNumber = Random().Next(0,n)
                offlineUsers <- Set.add onlineList.[randNumber] offlineUsers
                onlineList.[i] <! Offline
                onlineUsers <- Set.remove onlineList.[randNumber] onlineUsers
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(5.0),TimeSpan.FromMilliseconds(5000.0), mailbox.Self, SetOffline)
    

        | Tweet ->
            for userRef in userListRef do
                userRef <! Tweets
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromMilliseconds(1000.0), mailbox.Self, Tweet)
            
        | Query ->

            for userRef in userListRef do
                let rand= Random().Next(0,2)
                if rand = 0 then
                    userRef <! QueryHashTag
                else if rand = 1 then
                    userRef <! QueryMention
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromMilliseconds(500.0), mailbox.Self, Query)
                

        return! loop()
    }
    loop()
             

let clientActor = spawn system "ClientSpace" Client

clientActor <! SetParameters
system.WhenTerminated.Wait()