CREATE TABLE Trainers (
    TrainerID INT  PRIMARY KEY ,
    TrainerName VARCHAR(50) NOT NULL,
    Age INT
);

INSERT INTO Trainers (TrainerName,Age) values ("Patxi",19);
INSERT INTO Trainers (TrainerName,Age) values ("Paco",12);
INSERT INTO Trainers (TrainerName,Age) values ("Pepe",8);
INSERT INTO Trainers (TrainerName,Age) values ("Goku",99);

CREATE TABLE Pokemons (
    PokemonID INT  PRIMARY KEY,
    PokemonName VARCHAR(50) NOT NULL
);

INSERT INTO Pokemons (PokemonName) values ("RAICHI");
INSERT INTO Pokemons (PokemonName) values ("Jujanchi");
INSERT INTO Pokemons (PokemonName) values ("Raticate");

CREATE TABLE Badges (
    BadgeID INT  PRIMARY KEY,
    BadgeName VARCHAR(50) NOT NULL,
    TrainerID INT UNIQUE,
    FOREIGN KEY (TrainerID) REFERENCES Trainers(TrainerID) ON DELETE CASCADE
);

INSERT INTO Badges (BadgeName,TrainerID) values ("PRIMERO GYM",1);
INSERT INTO Badges (BadgeName,TrainerID) values ("SEGUNDO GYM",2);
INSERT INTO Badges (BadgeName,TrainerID) values ("TERCER GYM",3);


CREATE TABLE Abilities (
    AbilityID INT  PRIMARY KEY,
    AbilityName VARCHAR(50) NOT NULL
);

INSERT INTO Abilities (AbilityName) values ("dAMP");
INSERT INTO Abilities (AbilityName) values ("FireStrake");
INSERT INTO Abilities (AbilityName) values ("Joga Bonit");


CREATE TABLE PokemonAbilities (
    PokemonID INT,
    AbilityID INT,
    PRIMARY KEY (PokemonID, AbilityID),
    FOREIGN KEY (PokemonID) REFERENCES Pokemons(PokemonID) ON DELETE CASCADE,
    FOREIGN KEY (AbilityID) REFERENCES Abilities(AbilityID) ON DELETE CASCADE
);

INSERT INTO PokemonAbilities (PokemonID,AbilityID) values (1,1);
INSERT INTO PokemonAbilities (PokemonID,AbilityID) values (2,2);
INSERT INTO PokemonAbilities (PokemonID,AbilityID) values (3,3);

CREATE TABLE Battles (
    BattleID INT  PRIMARY KEY,
    Trainer1ID INT,
    Trainer2ID INT,
    WinnerTrainerID INT,
    BattleDate DATETIME,
    FOREIGN KEY (Trainer1ID) REFERENCES Trainers(TrainerID) ON DELETE CASCADE,
    FOREIGN KEY (Trainer2ID) REFERENCES Trainers(TrainerID) ON DELETE CASCADE,
    FOREIGN KEY (WinnerTrainerID) REFERENCES Trainers(TrainerID) ON DELETE SET NULL
);


INSERT INTO Battles (Trainer1ID,Trainer2ID,WinnerTrainerID,BattleDate) values (1,2,1,NOW());
INSERT INTO Battles (Trainer1ID,Trainer2ID,WinnerTrainerID,BattleDate) values (2,3,2,NOW());
INSERT INTO Battles (Trainer1ID,Trainer2ID,WinnerTrainerID,BattleDate) values (1,3,3,NOW());

CREATE TABLE BattlePokemons (
    BattleID INT,
    PokemonID INT,
    TrainerID INT,
    PRIMARY KEY (BattleID, PokemonID,TrainerID),
    FOREIGN KEY (BattleID) REFERENCES Battles(BattleID) ON DELETE CASCADE,
    FOREIGN KEY (PokemonID) REFERENCES Pokemons(PokemonID) ON DELETE CASCADE,
    FOREIGN KEY (TrainerID) REFERENCES Trainers(TrainerID) ON DELETE CASCADE
);

INSERT INTO BattlePokemons (BattleID,PokemonID,TrainerID) values (1,2,1);
INSERT INTO BattlePokemons (BattleID,PokemonID,TrainerID) values (2,3,2);
INSERT INTO BattlePokemons (BattleID,PokemonID,TrainerID) values (1,3,3);
INSERT INTO BattlePokemons (BattleID,PokemonID,TrainerID) values (2,2,1);

--Select all trainers.
select * from Trainers;
--Find all Pokémon names.
select PokemonName  FROM Pokemons;
--Count the number of Pokémon each trainer has.
select t.TrainerName, count(bp.PokemonID) as num_pokemons from Trainers t left join BattlePokemons bp on t.TrainerId=bp.TrainerId
group by t.TrainerName;

--Listar todas las medallas con los nombres de sus entrenadores correspondientes.
select b.BadgeName, t.TrainerName from Badges b left join Trainers t on b.TrainerID=t.TrainerID;
--Obtener todas las habilidades.
select * from Abilities;
--Encontrar todas las batallas y sus ganadores.

select b.BattleID,t.TrainerName from Battles b left join Trainers t on b.WinnerTrainerID=t.TrainerID;
--Obtener todos los Pokémon asociados con una batalla específica (por ejemplo, BattleID = 1).
select p.PokemonName from Pokemons p  join BattlePokemons bp on p.PokemonID=bp.PokemonID where bp.BattleID=1;
--Recuperar todos los Pokémon de un entrenador específico (por ejemplo, TrainerID = 1).
select p.PokemonName from Pokemons p join BattlePokemons bp on p.PokemonID=bp.PokemonID where bp.TrainerID=1 ;
--Listar todos los entrenadores que tienen al menos una medalla.
select t.TrainerName , count(b.BadgeID) as num_medallas  from Trainers t  join Badges b on t.TrainerID=b.BadgeID GROUP BY t.TrainerName;
--Contar el número de batallas en las que ha participado cada entrenador.
select t.TrainerName, count(DISTINCT bp.BattleID) as num_batallas 
from BattlePokemons bp INNER join Trainers t on t.TrainerID=bp.TrainerID GROUP BY t.TrainerName;
--Listar todas las medallas y los entrenadores que las tienen, incluyendo entrenadores sin medallas (LEFT JOIN).
select t.TrainerName, count(b.BadgeID) as num_medallas  from Trainers t left join Badges b on t.TrainerID=b.TrainerID 
group by t.TrainerName;
--Encontrar a todos los entrenadores y sus Pokémon, incluyendo entrenadores sin Pokémon (LEFT JOIN).
SELECT t.TrainerName,count(p.PokemonID) as num_pokemons from Trainers t left join BattlePokemons bp on t.TrainerID=bp.TrainerID left join Pokemons p on bp.PokemonID=p.PokemonID
GROUP BY t.TrainerName;
--Obtener una lista de entrenadores y sus medallas, incluyendo medallas sin entrenadores (RIGHT JOIN).
select b.BadgeName,t.TrainerName  from Badges b right join Trainers t on b.TrainerID=t.TrainerID ;

SELECT t.TrainerName, b.BadgeName 
FROM Badges b 
RIGHT JOIN Trainers t ON b.TrainerID = t.TrainerID;

--Combinar entrenadores que tienen al menos un Pokémon y aquellos que tienen medallas (UNION).
select t.TrainerName from Trainers t join BattlePokemons bp on t.TrainerID=bp.TrainerID
group by t.TrainerName
UNION
select t.TrainerName from Trainers t join Badges b on t.TrainerID=b.TrainerID
group by t.TrainerName;
--Obtener todos los entrenadores que tienen Pokémon o medallas (INTERSECT).
select t.TrainerName from Trainers t join BattlePokemons bp on t.TrainerID=bp.TrainerID
group by t.TrainerName
INTERSECT
select t.TrainerName from Trainers t join Badges b on t.TrainerID=b.TrainerID
group by t.TrainerName;
--Encontrar a todos los entrenadores con sus Pokémon y habilidades utilizando múltiples joins.
select t.TrainerName,p.PokemonName, a.AbilityName from Trainers t join BattlePokemons bp on t.TrainerID=bp.TrainerID 
join Pokemons p on bp.PokemonID=p.PokemonID 
join PokemonAbilities pa on p.PokemonID=pa.PokemonID
join Abilities a on pa.AbilityID=a.AbilityID;
--Encontrar al entrenador con más Pokémon.
select t.TrainerName, count(bp.PokemonID) as num_pokemon from Trainers t join BattlePokemons bp on t.TrainerID=bp.TrainerID
group by t.TrainerName
order by num_pokemon DESC 
--Listar todas las batallas que ocurrieron en una fecha específica (por ejemplo, '2024-01-01').
SELECT b.BattleID, b.Trainer1ID, b.Trainer2ID, b.WinnerTrainerID
FROM Battles b
WHERE DATE(b.BattleDate) = '2024-01-01';

--Recuperar todos los nombres de Pokémon junto con sus habilidades.
select p.PokemonName, a.AbilityName  from Pokemons p  join PokemonAbilities pa on p.PokemonID=pa.PokemonID
 join Abilities a on pa.AbilityID=a.AbilityID;
--Obtener a todos los entrenadores que ganaron una batalla.
select t.TrainerName,b.* from Trainers t join Battles b on t.TrainerID=b.WinnerTrainerID;
--Subconsulta de múltiples filas: Recuperar a todos los entrenadores que tienen más Pokémon que el promedio de Pokémon por entrenador y mostrar la media.
--Pokémon por entrenador

SELECT t.TrainerName, COUNT(bp.PokemonID) AS PokemonCount
FROM Trainers t
JOIN BattlePokemons bp ON t.TrainerID = bp.TrainerID
GROUP BY t.TrainerID, t.TrainerName
HAVING COUNT(bp.PokemonID) > (
    SELECT AVG(PokemonCount)
    FROM (
        SELECT COUNT(bp2.PokemonID) AS PokemonCount
        FROM Trainers t2
        LEFT JOIN BattlePokemons bp2 ON t2.TrainerID = bp2.TrainerID
        GROUP BY t2.TrainerID
    ) AS TrainerPokemonCounts
);

--Subconsulta correlacionada: Listar todos los entrenadores que tienen más Pokémon que el entrenador con el menor número de Pokémon.
SELECT t.TrainerName, COUNT(bp.PokemonID) AS num_pok
FROM Trainers t
LEFT JOIN BattlePokemons bp ON t.TrainerID = bp.TrainerID
GROUP BY t.TrainerID, t.TrainerName
HAVING COUNT(bp.PokemonID) > (
    SELECT MIN(PokemonCount)
    FROM (
        SELECT COUNT(bp2.PokemonID) AS PokemonCount
        FROM Trainers t2
        LEFT JOIN BattlePokemons bp2 ON t2.TrainerID = bp2.TrainerID
        GROUP BY t2.TrainerID
    ) AS TrainerPokemonCounts
);

--Subconsulta correlacionada: Encontrar todas las batallas donde el Entrenador1 tiene más Pokémon que el Entrenador2.
select b.BattleID, t1.TrainerName as Entrenador1, t2.TrainerName as Entrenador2 from Battles b join Trainers t1 on b.Trainer1ID=t1.TrainerID
join Trainers t2 on b.Trainer2ID=t2.TrainerID
WHERE
(select count(bp1.PokemonID) from BattlePokemons bp1 where bp1.TrainerID=t1.TrainerID)
>
(select count(bp2.PokemonID) from BattlePokemons bp2 where bp2.TrainerID=t2.TrainerID)
;
--Subconsulta de múltiples filas: Listar todos los Pokémon que tienen habilidades no poseídas por ningún Pokémon en una batalla específica (por ejemplo, BattleID = 1).

SELECT p.PokemonName
FROM Pokemons p
WHERE p.PokemonID IN (
    SELECT pa.PokemonID
    FROM PokemonAbilities pa
    WHERE pa.AbilityID NOT IN (
        SELECT pa2.AbilityID
        FROM BattlePokemons bp
        JOIN PokemonAbilities pa2 ON bp.PokemonID = pa2.PokemonID
        WHERE bp.BattleID = 1
    )
);

