/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.internal.publisher.MavenProjectIdentity
import org.gradle.api.publish.maven.tasks.AbstractPublishToMaven
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.jvm.tasks.Jar

/**
 * EhDeploy
 */
class EhDeploy implements Plugin<Project> {
  @Override
  void apply(Project project) {
    project.plugins.apply 'maven-publish'
    project.getExtensions().create(EhDeployExtension, 'deploy', EhDeployExtension, project)

    project.configurations {
      providedApi
      providedImplementation

      api.extendsFrom providedApi
      implementation.extendsFrom providedImplementation
    }

    project.publishing {
      repositories {
        if (project.isReleaseVersion) {
          maven {
            url = project.deployUrl
            credentials {
              username = project.deployUser
              password = project.deployPwd
            }
          }
        } else {
          maven {
            name = 'sonatype-nexus-snapshot'
            url = 'https://oss.sonatype.org/content/repositories/snapshots'
            credentials {
              username = project.sonatypeUser
              password = project.sonatypePwd
            }
          }
        }
      }

      publications.withType(MavenPublication).all {
        pom {
          withXml {
            asNode().dependencies.'*'.findAll() {
              ([project.configurations.providedApi, project.configurations.providedImplementation] +
                project.configurations.matching { conf -> conf.name in [EhVoltron.VOLTRON_CONFIGURATION_NAME, EhVoltron.SERVICE_CONFIGURATION_NAME] }).any {
                conf -> conf.allDependencies.find { dep -> dep.name == it.artifactId.text() }
              }
            }.each() {
              it.scope*.value = 'provided'
            }
          }
          url = 'http://ehcache.org'
          organization {
            name = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
            url = 'http://terracotta.org'
          }
          issueManagement {
            system = 'Github'
            url = 'https://github.com/ehcache/ehcache3/issues'
          }
          scm {
            url = 'https://github.com/ehcache/ehcache3'
            connection = 'scm:git:https://github.com/ehcache/ehcache3.git'
            developerConnection = 'scm:git:git@github.com:ehcache/ehcache3.git'
          }
          licenses {
            license {
              name = 'The Apache Software License, Version 2.0'
              url = 'http://www.apache.org/licenses/LICENSE-2.0.txt'
              distribution = 'repo'
            }
          }
          developers {
            developer {
              name = 'Terracotta Engineers'
              email = 'tc-oss@softwareag.com'
              organization = 'Terracotta Inc., a wholly-owned subsidiary of Software AG USA, Inc.'
              organizationUrl = 'http://ehcache.org'
            }
          }
        }
      }
    }

    project.tasks.withType(AbstractPublishToMaven).all { publishTask ->
      publishTask.doFirst {
        def component = publishTask.publication.component
        if (component != null) { //The shadow plugin doesn't associate a component with the publication
          def unpublishedDeps = component.usages.collectMany { usage ->
            usage.dependencies.withType(ProjectDependency).matching { !it.dependencyProject.plugins.hasPlugin(TcDeployPlugin) }
          }
          if (!unpublishedDeps.isEmpty()) {
            project.logger.warn("{} has applied the deploy plugin but has unpublished project dependencies: {}", project, unpublishedDeps)
          }
        }
      }
    }

    project.tasks.register('install') {
      dependsOn project.tasks.publishToMavenLocal
    }

    project.plugins.withId('java') {

      project.javadoc {
        title "$project.archivesBaseName $project.version API"
        exclude '**/internal/**'
        options.addStringOption('Xdoclint:none', '-quiet')
      }

      project.java {
        withJavadocJar()
        withSourcesJar()
      }

      project.afterEvaluate {
        if (project.publishing.publications.isEmpty()) {
          project.publishing {
            publications {
              mavenJava(MavenPublication) {
                from project.components.java
              }
            }
          }
        }

        project.tasks.withType(GenerateMavenPom).all { pomTask ->
          MavenProjectIdentity identity = pomTask.pom.projectIdentity
          project.tasks.withType(Jar) {
            from(pomTask) {
              into "META-INF/maven/${identity.groupId.get()}/${identity.artifactId.get()}"
              rename '.*', 'pom.xml'
            }
          }
        }
      }
    }
  }

  static class EhDeployExtension {

    final Project project
    final Collection<MavenPublication> mavenPublications

    EhDeployExtension(Project project) {
      this.project = project;
      this.mavenPublications = project.publishing.publications.withType(MavenPublication)
    }

    void setGroupId(String groupId) {
      mavenPublications.all {
        it.groupId = groupId
      }
    }

    void setArtifactId(String artifactId) {
      project.archivesBaseName = artifactId
      mavenPublications.all {
        it.artifactId = artifactId
      }
    }

    void setName(String name) {
      mavenPublications.all {
        it.pom.name = name
      }
    }

    void setDescription(String description) {
      mavenPublications.all {
        it.pom.description = description
      }
    }
  }
}
