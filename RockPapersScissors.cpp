// RockPapersScissors.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <ctime>
int game(std::string name);

int main()
{   
    int track1 = 0;
    int track2 = 0;
    std::string name;
    std::cout << "WELCOME TO ROCK PAPER SCISSORS GAME! The first player to win 3 games wins! \n";
    std::cout << "Insert your name please \n";
    std::cin >> name;
    do{
        int s = game(name);
        track1 = track1 + s;
        if (s == 0) { track2++; }
    } while (track1 < 3 || track2 < 3);
    std::cout << "The player " << name << " won " << track1 << " times.";
}
int game(std::string name) {
    int result;
    char value;
    int computer;
    bool run1 = true;
    do {
        std::cout << "Choose between: \n";
        std::cout << "R-Rock \n";
        std::cout << "P-Paper \n";
        std::cout << "S-Scissors \n";
        std::cin >> value;
        if (value == 'R' || value == 'P' || value == 'S') { run1 = false; }
    } while (run1);
    
    std::srand(std::time(nullptr));
    computer = std::rand() % 3 + 1;
    if (computer == 1) {
        std::cout << "Computer chose Rock! \n ";
    }
    else if (computer == 2) {
        std::cout << "Computer chose Paper! \n";
    }
    else if (computer == 3) {
        std::cout << "Computer chose Scissors! \n";
    }
    if (value == 'R' && computer == 1) {
        std::cout << "It's a tie ! \n";
        result = 0;
    }
    else if (value == 'R' && computer == 2) {
        std::cout << name << " loses ! \n";
        result = 0;
    }
    else if (value == 'R' && computer == 3) {
        std::cout << name << " wins !\n";
        result = 1;
    }
    else if (value == 'P' && computer == 1) {
        std::cout << name << " wins ! \n";
        result = 1;
    }
    else if (value == 'P' && computer == 2) {
        std::cout << "It's a tie ! \n";
        result = 0;
    }
    else if (value == 'P' && computer == 3) {
        std::cout << name << " loses ! \n";
        result = 0;
    }
    else if (value == 'S' && computer == 1) {
        std::cout << name << " loses ! \n";
        result = 0;
    }
    else if (value == 'S' && computer == 2) {
        std::cout << name << " wins ! \n";
        result = 1;
    }
    else if (value == 'S' && computer == 3) {
        std::cout << "It's a tie ! \n";
        result = 0;
    }
  return result;
}