plugins {
    id("java")
    id("application")
}

group = "com.crokoking"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(8)
    }
}

application {
    mainClass.set("com.crokoking.datastore.export.translator.Main")
}

tasks {
    create("runValidations", JavaExec::class) {
        mainClass.set("com.crokoking.datastore.export.translator.ValidatorMain")
        classpath = sourceSets["main"].runtimeClasspath
    }
}

@Suppress("VulnerableLibrariesLocal")
dependencies {
    implementation("org.iq80.leveldb:leveldb:0.12")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("com.google.appengine:appengine-api-1.0-sdk:2.0.12")
    implementation("commons-cli:commons-cli:1.6.0")
}

tasks.test {
    useJUnitPlatform()
}