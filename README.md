<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="https://i.imgur.com/FxL5qM0.jpg" alt="Bot logo"></a>
</p>

<h3 align="center">Zarka</h3>

<div align="center">

  [![Status](https://img.shields.io/static/v1?label=Status&message=Active&color=brightgreen)]()
  [![GitHub Issues](https://img.shields.io/github/issues/AhmedElemary57/Zarka-DataBase-System)](https://github.com/AhmedElemary57/Zarka-DataBase-System/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/AhmedElemary57/Zarka-DataBase-System)](https://github.com/AhmedElemary57/Zarka-DataBase-System/pulls)
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center"> 🟦 What is Zarka?
    <br> 
</p>

## 📝 Table of Contents
+ [What is Zarka?](#about)
+ [Demo / Working](#demo)
+ [How it works](#working)
+ [Usage](#usage)
+ [Getting Started](#getting_started)
+ [Deploying your own bot](#deployment)
+ [Built Using](#built_using)
+ [TODO](../TODO.md)
+ [Contributing](../CONTRIBUTING.md)
+ [Authors](#authors)
+ [Acknowledgments](#acknowledgement)

## 🤔 What is Zarka? <a name = "about"></a>
Zarka is a distributed, leaderless, scalable, Partitioned NoSQL database management system designed to handle large scales of data distributed across numerous networks and machines It was inspired by the architecture of apache cassandra DBMS To utilize the solutions and models that was designed by the cassandra research project authors.

## 🎥 Demo / Working <a name = "demo"></a>
![Working](https://media.giphy.com/media/20NLMBm0BkUOwNljwv/giphy.gif)

## 💭 How it works <a name = "working"></a>

Zarka consists of:
1. Client:
    * The client is responsible for the set and get requests.
    * Every request consists of (Key, Value).
    * This request is sent to a random server then this server becomes the coordinator.
2. Servers:
    * When the server receives a request, it acts as the coordinator server.
    * Because of the partitioning among the servers using consistent hashing, the coordinator server knows which servers will deal with the request.
    * While applying leaderless replication, the coordinator server sends the request to n replicas (n depends on the quorum factor).
    * Servers then handles the set/get request.
     
3. Admin configuration panel:
    * The Admin is responsible for:
        * Setting up the system configuration (e.g., number of servers, virtual nodes, quorum factor, etc.).
        * Adding a new server to the system.
        * Visualize the distribution of the data among servers statistically.


## 🎈 Usage <a name = "usage"></a>

To use the bot, type:
```
!dict word
```
The first part, i.e. "!dict" **is not** case sensitive.

The bot will then give you the Oxford Dictionary (or Urban Dictionary; if the word does not exist in the Oxford Dictionary) definition of the word as a comment reply.

### Example:

> !dict what is love

**Definition:**

Baby, dont hurt me~
Dont hurt me~ no more.

**Example:**

Dude1: Bruh, what is love?
Dude2: Baby, dont hurt me, dont hurt me- no more!
Dude1: dafuq?

**Source:** https://www.urbandictionary.com/define.php?term=what%20is%20love

---

<sup>Beep boop. I am a bot. If there are any issues, contact my [Master](https://www.reddit.com/message/compose/?to=PositivePlayer1&subject=/u/Wordbook_Bot)</sup>

<sup>Want to make a similar reddit bot? Check out: [GitHub](https://github.com/kylelobo/Reddit-Bot)</sup>

## 🏁 Getting Started <a name = "getting_started"></a>
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them.

```
Give examples
```

### Installing

A step by step series of examples that tell you how to get a development env running.

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo.

## 🚀 Deploying your own bot <a name = "deployment"></a>
To see an example project on how to deploy your bot, please see my own configuration:

+ **Heroku**: https://github.com/kylelobo/Reddit-Bot#deploying_the_bot

## ⛏️ Built Using <a name = "built_using"></a>
+ [PRAW](https://praw.readthedocs.io/en/latest/) - Python Reddit API Wrapper
+ [Heroku](https://www.heroku.com/) - SaaS hosting platform

## ✍️ Authors <a name = "authors"></a>
+ [@kylelobo](https://github.com/kylelobo) - Idea & Initial work

See also the list of [contributors](https://github.com/kylelobo/The-Documentation-Compendium/contributors) who participated in this project.

## 🎉 Acknowledgements <a name = "acknowledgement"></a>
+ Hat tip to anyone whose code was used
+ Inspiration
+ References
