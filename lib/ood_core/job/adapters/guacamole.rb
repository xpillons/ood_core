require "ood_core/refinements/hash_extensions"
require "ood_core/refinements/array_extensions"

require "json"
require "securerandom"
require "tempfile"

module OodCore
  module Job
    class Factory
      using Refinements::HashExtensions

      def self.build_guacamole(config)
          Adapters::Guacamole.new(config)
      end
    end

    module Adapters
      class Guacamole < Adapter

        using Refinements::ArrayExtensions
        using Refinements::HashExtensions

        attr_accessor :guac_spool

        def initialize(opts = {})
          @guac_spool = (ENV['GUAC_SPOOLER_DIRECTORY'] || "/anfhome/guac-spool")
        end

        # Submit a job with the attributes defined in the job template instance
        # @abstract Subclass is expected to implement {#submit}
        # @raise [NotImplementedError] if subclass did not define {#submit}
        # @example Submit job template to cluster
        #   solver_id = job_adapter.submit(solver_script)
        #   #=> "1234.server"
        # @example Submit job that depends on previous job
        #   post_id = job_adapter.submit(
        #     post_script,
        #     afterok: solver_id
        #   )
        #   #=> "1235.server"
        # @param script [Script] script object that describes the
        #   script and attributes for the submitted job
        # @param after [#to_s, Array<#to_s>] this job may be scheduled for execution
        #   at any point after dependent jobs have started execution
        # @param afterok [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution only after dependent jobs have terminated with no errors
        # @param afternotok [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution only after dependent jobs have terminated with errors
        # @param afterany [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution after dependent jobs have terminated
        # @return [String] the job id returned after successfully submitting a job
        def submit(script, after: [], afterok: [], afternotok: [], afterany: [])
          # need to set the following env vars for the scripts
          # - GUACAMOLE_SPOOL_DIR
          # - GUACAMOLE_SESSION_ID
          uuid = SecureRandom.uuid 
          
          env = {
            "GUACAMOLE_SPOOL_DIR" => guac_spool,
            "GUACAMOLE_SESSION_ID" => uuid
          }
          cmd = "/usr/bin/bash"

          content = "#{script.content}"

          o, e, s = Open3.capture3(env, cmd, stdin_data: content)
          
          s.success? ? uuid.to_s : raise(Error, e)
        end


        # Retrieve info for all jobs from the resource manager
        # @abstract Subclass is expected to implement {#info_all}
        # @raise [NotImplementedError] if subclass did not define {#info_all}
        # @param attrs [Array<symbol>] defaults to nil (and all attrs are provided) 
        #   This array specifies only attrs you want, in addition to id and status.
        #   If an array, the Info object that is returned to you is not guarenteed
        #   to have a value for any attr besides the ones specified and id and status.
        #
        #   For certain adapters this may speed up the response since
        #   adapters can get by without populating the entire Info object
        # @return [Array<Info>] information describing submitted jobs
        def info_all(attrs: nil)
          jobs = []
          Dir.foreach(guac_spool+"/status") do |filename|
            next if filename == "." or filename == ".."
            jobs.append(
              read_status(filename)
            )
          end
          jobs
        end

        # Retrieve info for all jobs for a given owner or owners from the
        # resource manager
        # @param owner [#to_s, Array<#to_s>] the owner(s) of the jobs
        # @param attrs [Array<symbol>] defaults to nil (and all attrs are provided) 
        #   This array specifies only attrs you want, in addition to id and status.
        #   If an array, the Info object that is returned to you is not guarenteed
        #   to have a value for any attr besides the ones specified and id and status.
        #
        #   For certain adapters this may speed up the response since
        #   adapters can get by without populating the entire Info object
        # @return [Array<Info>] information describing submitted jobs
        def info_where_owner(owner, attrs: nil)
          owner = Array.wrap(owner).map(&:to_s)

          # must at least have job_owner to filter by job_owner
          attrs = Array.wrap(attrs) | [:job_owner] unless attrs.nil?

          info_all(attrs: attrs).select { |info| owner.include? info.job_owner }
        end

        # Iterate over each job Info object
        # @param attrs [Array<symbol>] defaults to nil (and all attrs are provided) 
        #   This array specifies only attrs you want, in addition to id and status.
        #   If an array, the Info object that is returned to you is not guarenteed
        #   to have a value for any attr besides the ones specified and id and status.
        #
        #   For certain adapters this may speed up the response since
        #   adapters can get by without populating the entire Info object
        # @yield [Info] of each job to block
        # @return [Enumerator] if no block given
        def info_all_each(attrs: nil)
          return to_enum(:info_all_each, attrs: attrs) unless block_given?

          info_all(attrs: attrs).each do |job|
            yield job
          end
        end

        # Iterate over each job Info object
        # @param owner [#to_s, Array<#to_s>] the owner(s) of the jobs
        # @param attrs [Array<symbol>] defaults to nil (and all attrs are provided) 
        #   This array specifies only attrs you want, in addition to id and status.
        #   If an array, the Info object that is returned to you is not guarenteed
        #   to have a value for any attr besides the ones specified and id and status.
        #
        #   For certain adapters this may speed up the response since
        #   adapters can get by without populating the entire Info object
        # @yield [Info] of each job to block
        # @return [Enumerator] if no block given
        def info_where_owner_each(owner, attrs: nil)
          return to_enum(:info_where_owner_each, owner, attrs: attrs) unless block_given?

          info_where_owner(owner, attrs: attrs).each do |job|
            yield job
          end
        end

        # Whether the adapter supports job arrays
        # @return [Boolean] - assumes true; but can be overridden by adapters that
        #   explicitly do not
        def supports_job_arrays?
          false
        end

        # Retrieve job info from the resource manager
        # @abstract Subclass is expected to implement {#info}
        # @raise [NotImplementedError] if subclass did not define {#info}
        # @param id [#to_s] the id of the job
        # @return [Info] information describing submitted job
        def info(id)
          filename = id.to_s+'.json'
          if File.exists?(guac_spool+"/status/"+filename)
            info = read_status(filename)  
          else
            info = Info.new(
              id: id, 
              status: "completed" 
            )
          end
          info
        end

        # Retrieve job status from resource manager
        # @note Optimized slightly over retrieving complete job information from server
        # @abstract Subclass is expected to implement {#status}
        # @raise [NotImplementedError] if subclass did not define {#status}
        # @param id [#to_s] the id of the job
        # @return [Status] status of job
        def status(id)
          info(id).status
        end

        # Put the submitted job on hold
        # @abstract Subclass is expected to implement {#hold}
        # @raise [NotImplementedError] if subclass did not define {#hold}
        # @param id [#to_s] the id of the job
        # @return [void]
        def hold(id)
          raise NotImplementedError, 'subclass did not define #hold'
        end

        # Release the job that is on hold
        # @abstract Subclass is expected to implement {#release}
        # @raise [NotImplementedError] if subclass did not define {#release}
        # @param id [#to_s] the id of the job
        # @return [void]
        def release(id)
          raise NotImplementedError, 'subclass did not define #release'
        end

        # Delete the submitted job.
        #
        # @param id [#to_s] the id of the job
        # @return [void]
        def delete(id)
          command = {
            "command" => "delete"
          }
          File.open(guac_spool+"/commands/"+id.to_s+".json", "w") do |f|
            f.write(command.to_json)
          end
        end

        private
          # Read in status
          def read_status(filename)
            file = File.read(guac_spool+"/status/"+filename)
            json = JSON.parse(file)

            status = json['status']
            if status != 'running' and status != 'completed' and status != 'queued' and status != 'failed'
              status = 'undetermined'
            end

            Info.new(
              id: File.basename(filename, File.extname(filename)), 
              status: status, 
              job_name: json['jobname'],
              job_owner: json['user'],
              queue_name: json['queuename'],
              native: json
            )
          end

      end
    end
  end
end
