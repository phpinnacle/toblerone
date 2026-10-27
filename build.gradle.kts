group = "org.phpinnacle.toblerone"
version = System.getenv("VERSION") ?: "1.0.0"

val javaVersion = 17

val artifactoryContext =
    project.properties.getOrDefault("artifactory_context", System.getenv("ARTIFACTORY_CONTEXT")).toString()
val artifactoryUsern =
    project.properties.getOrDefault("artifactory_user", System.getenv("ARTIFACTORY_USER")).toString()
val artifactoryPassword =
    project.properties.getOrDefault("artifactory_password", System.getenv("ARTIFACTORY_PWD")).toString()

plugins {
    kotlin("jvm") version "1.9.25"
    idea // Generates files that are used by IntelliJ IDEA, thus making it possible to open the project from IDEA
    `java-library` // Apply the java-library plugin for API and implementation separation.
    `maven-publish`
    id("io.gitlab.arturbosch.detekt") version "1.23.6"
}

repositories {
    mavenCentral()
}

dependencies {
    val kafkaConnectVersion = "3.9.0"

    compileOnly("org.apache.kafka:connect-api:$kafkaConnectVersion")
    compileOnly("org.apache.kafka:connect-transforms:$kafkaConnectVersion")
    compileOnly("org.apache.kafka:connect-json:$kafkaConnectVersion")
    compileOnly("io.debezium:debezium-api:3.0.7.Final") {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    compileOnly("org.postgresql:postgresql:42.6.1")
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.16.2")

    implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation("com.ibm.icu:icu4j:75.1")

    testImplementation("org.apache.kafka:connect-runtime:$kafkaConnectVersion")
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
}

configurations.testImplementation {
    extendsFrom(configurations.compileOnly.get())
}

kotlin {
    jvmToolchain(javaVersion)
}

tasks {
    val fatJar = register<Jar>("fatJar") {
        dependsOn("classes")
        archiveClassifier.set("all")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
        val sourcesMain = sourceSets.main.get()
        val contents = configurations.runtimeClasspath.get()
            .map { if (it.isDirectory) it else zipTree(it) } +
                sourcesMain.output
        from(contents)
    }
    test {
        useJUnitPlatform()
        dependsOn(fatJar)
        inputs.file(fatJar.flatMap { it.archiveFile })
        systemProperty("plugin.jar", fatJar.get().archiveFile.get().asFile.absolutePath)
    }
    build {
        dependsOn(fatJar) // Trigger fat jar creation during build
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            pom {
                name.set( "phpinncle-toblerone")
                description.set( project.description)
                url.set("https://github.com/phpinnacle/toblerone")
                organization {
                    name.set("phpinnacle")
                    url.set("https://github.com/phpinnacle")
                }
                issueManagement {
                    system.set("GitHub")
                    url.set("https://github.com/phpinnacle/toblerone/issues")
                }
                licenses {
                    license {
                        name.set( "Apache License 2.0")
                        url.set("https://github.com/phpinnacle/toblerone/blob/master/LICENSE")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        name.set("phpinnacle")
                    }
                }
                scm {
                    url.set("https://github.com/phpinnacle/toblerone")
                    connection.set("scm:git:git://github.com/phpinnacle/toblerone.git")
                    developerConnection.set("scm:git:ssh://git@github.com:phpinnacle/toblerone.git")
                }
            }

            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ArtifactoryLocal"
            url = uri(artifactoryContext + "/libs-release-local")
            credentials {
                username = artifactoryUsern
                password = artifactoryPassword
            }
        }
    }
}

